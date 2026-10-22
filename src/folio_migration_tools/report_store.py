"""SQLite persistence for migration reports and data issues.

One database is written per task run. Re-running a task on the same iteration
replaces the database, mirroring how the report and log files are overwritten.
"""

import ast
import hashlib
import json
import logging
import re
import sqlite3
import weakref
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
ISSUE_FLUSH_SIZE = 5000

SCHEMA = """
CREATE TABLE schema_info (version INTEGER NOT NULL);
CREATE TABLE run (
    run_id INTEGER PRIMARY KEY,
    task_name TEXT,
    task_type TEXT,
    started TEXT,
    finished TEXT,
    tool_version TEXT,
    locale_at_run TEXT,
    config_json TEXT,
    mapping_total_records INTEGER
);
CREATE TABLE report_stat (
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    section_id TEXT NOT NULL,
    measure_key TEXT NOT NULL,
    params_json TEXT NOT NULL DEFAULT '{}',
    translated INTEGER NOT NULL DEFAULT 1,  -- 0 literal, 1 key, 2 composed parts
    count INTEGER NOT NULL,
    PRIMARY KEY (run_id, section_id, measure_key, params_json)
);
CREATE TABLE mapping_stat (
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    side TEXT NOT NULL,  -- 'folio' or 'legacy'
    field_name TEXT NOT NULL,
    present INTEGER,  -- legacy side only: records where the field was present
    mapped INTEGER NOT NULL,  -- records where the field was mapped
    PRIMARY KEY (run_id, side, field_name)
);
CREATE TABLE reference_content (
    sha256 TEXT PRIMARY KEY,
    content BLOB NOT NULL  -- zlib-compressed bytes of the file, as it was read
);
CREATE TABLE reference_file (
    ref_pk INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    role TEXT NOT NULL,  -- what the file was used for, e.g. a FOLIO property or 'mapping'
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL REFERENCES reference_content(sha256),
    size INTEGER NOT NULL,
    format TEXT,  -- 'tsv' or 'json'
    header_json TEXT,
    captured_at TEXT,
    UNIQUE (run_id, role, path)
);
CREATE TABLE reference_row (
    ref_pk INTEGER NOT NULL REFERENCES reference_file(ref_pk),
    row_index INTEGER NOT NULL,
    row_json TEXT NOT NULL,
    PRIMARY KEY (ref_pk, row_index)
);
CREATE TABLE legacy_record (
    record_pk INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    record_id TEXT,
    record_id_kind TEXT,
    source_file TEXT,
    source_index INTEGER,
    format TEXT,
    payload BLOB,
    payload_hash TEXT,
    masked INTEGER NOT NULL DEFAULT 0,
    mask_note TEXT,
    UNIQUE (run_id, source_file, source_index)
);
CREATE TABLE record_alias (
    record_pk INTEGER NOT NULL REFERENCES legacy_record(record_pk),
    legacy_id TEXT NOT NULL
);
CREATE TABLE data_issue (
    id INTEGER PRIMARY KEY,
    run_id INTEGER NOT NULL REFERENCES run(run_id),
    record_pk INTEGER REFERENCES legacy_record(record_pk),
    record_id TEXT,
    record_id_kind TEXT,
    record_id_raw TEXT,
    issue_type TEXT NOT NULL,
    message_key TEXT,
    params_json TEXT,
    translated INTEGER NOT NULL DEFAULT 0,
    legacy_value TEXT,
    location_kind TEXT,
    location_path TEXT,
    ts TEXT
);
CREATE INDEX ix_data_issue_msg ON data_issue (run_id, message_key);
CREATE INDEX ix_data_issue_record ON data_issue (record_pk);
CREATE INDEX ix_data_issue_record_id ON data_issue (record_id);
CREATE INDEX ix_record_alias ON record_alias (legacy_id);
CREATE INDEX ix_report_stat_section ON report_stat (run_id, section_id);
"""


@dataclass
class RecordRef:
    """A normalized reference to a legacy record as passed to the data issues log."""

    ids: list = field(default_factory=list)
    kind: str = "none"
    raw: str = ""

    @property
    def primary(self) -> str:
        return self.ids[0] if self.ids else ""


def normalize_record_ref(value: Any) -> RecordRef:
    """Normalize the many shapes of "index or id" into a RecordRef.

    Lists and tuples become their non-empty members, list-repr strings are
    unpacked, ints are row numbers, and empty values are "none".
    """
    if value is None or value == "" or value == [] or value == ():
        return RecordRef([], "none", "")
    raw = value if isinstance(value, str) else repr(value)
    if isinstance(value, bool):
        return RecordRef([str(value)], "legacy_id", raw)
    if isinstance(value, int):
        return RecordRef([str(value)], "row_number", raw)
    if isinstance(value, (list, tuple)):
        ids = [str(v) for v in value if v not in (None, "")]
        return RecordRef(ids, "legacy_id" if ids else "none", raw)
    text = str(value)
    stripped = text.strip()
    if stripped.startswith("[") and stripped.endswith("]"):
        try:
            parsed = ast.literal_eval(stripped)
        except (ValueError, SyntaxError):
            parsed = None
        if isinstance(parsed, (list, tuple)) and all(
            isinstance(p, (str, int, float)) for p in parsed
        ):
            ids = [str(p) for p in parsed if p != ""]
            return RecordRef(ids, "legacy_id" if ids else "none", raw)
    if stripped.isdigit() and not stripped.startswith("0"):
        return RecordRef([stripped], "legacy_id", raw)
    return RecordRef([text], "legacy_id", raw)


def describe_text(value: Any) -> tuple:
    """Return (key_or_text, params_json, translated) for a measure or issue message.

    translated is 1 for a TranslatedStr (key + params), 2 for a ComposedStr (the
    params_json holds its parts, each a translation key or literal text), and 0
    for plain text, which is stored as is.
    """
    parts = getattr(value, "parts", None)
    key = getattr(value, "key", None)
    if key:
        params = getattr(value, "params", None)
        return key, json.dumps(params, sort_keys=True, default=str) if params else None, 1
    if parts is not None:
        encoded = [
            {"key": p.key, "params": p.params} if getattr(p, "key", None) else {"text": str(p)}
            for p in parts
        ]
        return str(value), json.dumps({"parts": encoded}, sort_keys=True, default=str), 2
    return str(value), None, 0


class ReportStore:
    """Owns the SQLite connection and buffers writes for one task run."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        if not self.db_path.parent.is_dir():
            raise FileNotFoundError(
                f"Report database folder does not exist: {self.db_path.parent}"
            )
        self._remove_existing()
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.executescript(SCHEMA)
        self.conn.execute("INSERT INTO schema_info (version) VALUES (?)", (SCHEMA_VERSION,))
        self.conn.commit()
        self.run_id: int | None = None
        self._issue_buffer: list = []
        self._capture_records = True
        self._mask_fields: list[str] = []
        self._current: dict | None = None
        self.current_field: tuple | None = None
        self._reports: "weakref.WeakSet" = weakref.WeakSet()
        self.closed = False

    def register_report(self, report):
        """Track a MigrationReport so its stats are written when the run is finalized."""
        self._reports.add(report)

    def finalize(self, finished: str):
        """Write the stats of all registered reports, finish the run and close."""
        if self.closed:
            return
        for report in self._reports:
            self.write_report_stats(report.report)
        self.finish_run(finished)
        self.close()

    def _remove_existing(self):
        for suffix in ("", "-wal", "-shm"):
            Path(str(self.db_path) + suffix).unlink(missing_ok=True)

    def start_run(
        self,
        task_name: str,
        task_type: str = "",
        started: str = "",
        tool_version: str = "",
        locale: str = "",
        config: dict | None = None,
    ) -> int:
        cur = self.conn.execute(
            "INSERT INTO run (task_name, task_type, started, tool_version, locale_at_run,"
            " config_json) VALUES (?, ?, ?, ?, ?, ?)",
            (
                task_name,
                task_type,
                started,
                tool_version,
                locale,
                json.dumps(scrub_secrets(config), default=str) if config is not None else None,
            ),
        )
        self.conn.commit()
        self.run_id = cur.lastrowid
        return self.run_id

    def finish_run(self, finished: str):
        self.flush()
        self.conn.execute("UPDATE run SET finished = ? WHERE run_id = ?", (finished, self.run_id))
        self.conn.commit()

    def write_report_stats(self, report: dict):
        """Persist an in-memory MigrationReport.report dict as report_stat rows."""
        rows = []
        for section_id, measures in report.items():
            for measure, count in measures.items():
                if measure == "blurb_id":
                    continue
                text, params_json, translated = describe_text(measure)
                rows.append(
                    (self.run_id, section_id, text, params_json or "{}", translated, count)
                )
        if logger.isEnabledFor(logging.DEBUG):
            for row in rows:
                if not row[4]:
                    logger.debug("Untranslated report measure in %s: %r", row[1], row[2])
        self.conn.executemany(
            "INSERT INTO report_stat (run_id, section_id, measure_key, params_json,"
            " translated, count) VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (run_id, section_id, measure_key, params_json)"
            " DO UPDATE SET count = count + excluded.count",
            rows,
        )
        self.conn.commit()

    def write_mapping_stats(self, total_records: int, folio_fields: dict, legacy_fields: dict):
        """Persist the mapped FOLIO and legacy field counts of the mapping report.

        folio_fields maps a FOLIO property to [mapped]; legacy_fields maps a legacy field
        to [present, mapped]. Replaces any rows written earlier in the run.
        """
        rows = [(self.run_id, "folio", name, None, int(v[0])) for name, v in folio_fields.items()]
        rows += [
            (self.run_id, "legacy", name, int(v[0]), int(v[1]))
            for name, v in legacy_fields.items()
        ]
        self.conn.execute("DELETE FROM mapping_stat WHERE run_id = ?", (self.run_id,))
        self.conn.executemany(
            "INSERT INTO mapping_stat (run_id, side, field_name, present, mapped)"
            " VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.execute(
            "UPDATE run SET mapping_total_records = ? WHERE run_id = ?",
            (total_records, self.run_id),
        )
        self.conn.commit()

    def capture_reference_file(
        self, role: str, path: Path, rows: list[dict] | None = None, captured_at: str = ""
    ):
        """Snapshot a reference data file as it was when the task used it.

        The raw bytes are stored once per distinct content (compressed). For delimited
        files, pass the parsed rows to make the content queryable.
        """
        path = Path(path)
        data = path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        header = list(rows[0].keys()) if rows else None
        self.conn.execute(
            "INSERT OR IGNORE INTO reference_content (sha256, content) VALUES (?, ?)",
            (digest, zlib.compress(data)),
        )
        self.conn.execute(
            "DELETE FROM reference_row WHERE ref_pk IN"
            " (SELECT ref_pk FROM reference_file WHERE run_id = ? AND role = ? AND path = ?)",
            (self.run_id, role, str(path)),
        )
        self.conn.execute(
            "INSERT OR REPLACE INTO reference_file (run_id, role, path, sha256, size, format,"
            " header_json, captured_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.run_id,
                role,
                str(path),
                digest,
                len(data),
                "tsv" if rows is not None else path.suffix.lstrip(".").lower(),
                json.dumps(header) if header is not None else None,
                captured_at,
            ),
        )
        if rows:
            ref_pk = self.conn.execute(
                "SELECT ref_pk FROM reference_file WHERE run_id = ? AND role = ? AND path = ?",
                (self.run_id, role, str(path)),
            ).fetchone()[0]
            self.conn.executemany(
                "INSERT INTO reference_row (ref_pk, row_index, row_json) VALUES (?, ?, ?)",
                [(ref_pk, i, json.dumps(row)) for i, row in enumerate(rows)],
            )
        self.conn.commit()

    def configure_privacy(self, capture_legacy_records: bool = True, mask_fields=()):
        """Set whether legacy records are stored and which fields are masked."""
        self._capture_records = capture_legacy_records
        self._mask_fields = [f.strip() for f in mask_fields if f and f.strip()]

    def _mask_payload(self, payload: Any, fmt: str) -> tuple[Any, bool]:
        """Return the payload with masked fields replaced, and whether anything was masked.

        If masking is configured but cannot be applied, the payload is dropped rather
        than stored unmasked.
        """
        if payload is None or not self._mask_fields:
            return payload, False
        try:
            if fmt == "marc":
                return _mask_marc(payload, self._mask_fields), True
            if fmt == "json":
                return _mask_json(payload, self._mask_fields), True
        except Exception:
            logger.warning("Could not mask legacy record; it will not be stored")
        return None, True

    def _is_masked_location(self, kind: str | None, path: str | None) -> bool:
        if not self._mask_fields or not kind or not path:
            return False
        if kind == "column":
            return path in self._mask_fields
        tag, sub = _split_marc_path(path)
        return tag in self._mask_fields or (
            sub is not None and f"{tag}${sub}" in self._mask_fields
        )

    def set_current_record(
        self,
        record_id: Any,
        source_file: str = "",
        source_index: int = 0,
        format: str = "",
        payload_getter: Callable[[], Any] | None = None,
        kind: str | None = None,
    ):
        """Mark the record being processed. Nothing is written until an issue is logged.

        payload_getter is called lazily, once, when the first issue for this record
        is added, so clean records cost only this pointer assignment.
        """
        ref = normalize_record_ref(record_id)
        if kind and ref.ids:
            ref.kind = kind
        self._current = {
            "ref": ref,
            "source_file": source_file,
            "source_index": source_index,
            "format": format,
            "getter": payload_getter,
            "pk": None,
        }

    def add_current_ids(self, ids: Any):
        """Add ids (such as the legacy id found while mapping) to the current record."""
        if self._current is None:
            return
        ref = self._current["ref"]
        extra = [i for i in normalize_record_ref(ids).ids if i not in ref.ids]
        if extra and self._current["pk"] is None:
            ref.ids.extend(extra)
            ref.kind = "legacy_id"

    def clear_current_record(self):
        self._current = None

    def _current_record_pk(self) -> int | None:
        cur = self._current
        if cur is None:
            return None
        if cur["pk"] is None:
            payload = cur["getter"]() if cur["getter"] and self._capture_records else None
            payload, masked = self._mask_payload(payload, cur["format"])
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            note = None
            if masked:
                note = (
                    "Fields masked; refer to the original data file at "
                    f"{cur['source_file']}, index {cur['source_index']}"
                )
            ids = sorted(cur["ref"].ids)
            row = self.conn.execute(
                "INSERT INTO legacy_record (run_id, record_id, record_id_kind, source_file,"
                " source_index, format, payload, payload_hash, masked, mask_note)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT (run_id, source_file, source_index) DO UPDATE SET"
                " record_id = excluded.record_id RETURNING record_pk",
                (
                    self.run_id,
                    ids[0] if ids else "",
                    cur["ref"].kind,
                    cur["source_file"],
                    cur["source_index"],
                    cur["format"],
                    payload,
                    hashlib.sha256(payload).hexdigest() if payload else None,
                    int(masked),
                    note,
                ),
            ).fetchone()
            cur["pk"] = row[0]
            if len(ids) > 1:
                self.conn.executemany(
                    "INSERT INTO record_alias (record_pk, legacy_id) VALUES (?, ?)",
                    [(cur["pk"], i) for i in ids],
                )
        return cur["pk"]

    def add_issue(
        self,
        issue_type: str,
        record: Any,
        message: Any,
        legacy_value: Any,
        location_kind: str | None = None,
        location_path: str | None = None,
        ts: str = "",
    ):
        ref = normalize_record_ref(record)
        record_pk = None
        if self._current is not None:
            current_ids = set(self._current["ref"].ids)
            if not ref.ids:
                ref = self._current["ref"]
            if set(ref.ids) & current_ids or self._current["ref"].kind == "file_index":
                record_pk = self._current_record_pk()
        if len(ref.ids) > 1:
            ref = RecordRef(sorted(ref.ids), ref.kind, ref.raw)
        text, params_json, translated = describe_text(message)
        if self._is_masked_location(location_kind, location_path):
            original = None if legacy_value is None else str(legacy_value)
            if original:
                text = text.replace(original, MASK_TOKEN) if text else text
                if params_json:
                    params_json = params_json.replace(json.dumps(original)[1:-1], MASK_TOKEN)
            legacy_value = MASK_TOKEN
        self._issue_buffer.append(
            (
                self.run_id,
                record_pk,
                ref.primary,
                ref.kind,
                ref.raw if ref.raw != ref.primary else None,
                issue_type,
                text,
                params_json,
                translated,
                None if legacy_value is None else str(legacy_value),
                location_kind,
                location_path,
                ts,
            )
        )
        if len(self._issue_buffer) >= ISSUE_FLUSH_SIZE:
            self.flush()

    def flush(self):
        if self._issue_buffer:
            if logger.isEnabledFor(logging.DEBUG):
                for text in {r[6] for r in self._issue_buffer if not r[8]}:
                    logger.debug("Untranslated data issue message: %r", text)
            self.conn.executemany(
                "INSERT INTO data_issue (run_id, record_pk, record_id, record_id_kind,"
                " record_id_raw, issue_type, message_key, params_json, translated,"
                " legacy_value, location_kind, location_path, ts)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                self._issue_buffer,
            )
            self._issue_buffer.clear()
        self.conn.commit()

    def close(self):
        if self.closed:
            return
        self.flush()
        self.conn.close()
        self.closed = True


_active_store: ReportStore | None = None


MASK_TOKEN = "***MASKED***"  # noqa: S105
_SECRET_KEY = re.compile(r"pass(word|wd)|secret|token|api[_-]?key|credential", re.IGNORECASE)


def scrub_secrets(value: Any) -> Any:
    """Return a copy of value with the values of secret-like keys replaced by the mask token."""
    if isinstance(value, dict):
        return {
            k: MASK_TOKEN if isinstance(k, str) and _SECRET_KEY.search(k) else scrub_secrets(v)
            for k, v in value.items()
        }
    if isinstance(value, list):
        return [scrub_secrets(v) for v in value]
    return value


def _split_marc_path(path: str) -> tuple[str, str | None]:
    base = path.split("/", 1)[0]
    tag = re.split(r"[\[$]", base, maxsplit=1)[0]
    sub = base.split("$", 1)[1] if "$" in base else None
    return tag, sub


def _mask_marc(payload: bytes, mask_fields: list[str]) -> bytes:
    import pymarc

    record = pymarc.Record(data=payload, to_unicode=True, force_utf8=True)
    for spec in mask_fields:
        tag, _, code = spec.partition("$")
        for marc_field in record.get_fields(tag):
            if marc_field.is_control_field():
                marc_field.data = MASK_TOKEN
            elif not code:
                marc_field.subfields = [
                    pymarc.Subfield(sf.code, MASK_TOKEN) for sf in marc_field.subfields
                ]
            else:
                marc_field.subfields = [
                    pymarc.Subfield(sf.code, MASK_TOKEN if sf.code == code else sf.value)
                    for sf in marc_field.subfields
                ]
    return record.as_marc()


def _mask_json(payload: Any, mask_fields: list[str]) -> str:
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
    if isinstance(data, dict):
        data = {
            k: MASK_TOKEN if k in mask_fields and v not in (None, "") else v
            for k, v in data.items()
        }
    return json.dumps(data, default=str)


def marc_location(
    tag: str,
    subfield: str | None = None,
    occurrence: int | None = None,
    span: int | tuple[int, int] | None = None,
) -> tuple:
    """Build a (kind, path) location for a MARC field, subfield or character span.

    The path grammar is TAG[OCCURRENCE]$CODE/SPAN. The occurrence (zero-based index among
    fields with the same tag), the subfield code and the span are optional: 245, 245$a,
    245[0]$a, LDR/05, 008/35-37. The span follows the MARC convention: zero-based, inclusive
    character positions within the value of the field (or subfield), given as N or (N, M).
    If no occurrence is given and the field being processed (see iter_fields) has the
    same tag, its occurrence is used.
    """
    if occurrence is None and (store := get_active_store()) and store.current_field:
        current_tag, current_occurrence = store.current_field
        if current_tag == tag:
            occurrence = current_occurrence
    path = tag
    if occurrence is not None:
        path += f"[{occurrence}]"
    if subfield:
        path += f"${subfield}"
    if isinstance(span, tuple):
        path += f"/{span[0]}-{span[1]}"
    elif span is not None:
        path += f"/{span}"
    return "marc", path


def capture_reference_file(role: str, path: Path, rows: list[dict] | None = None):
    """Snapshot a reference file into the active store. Never raises."""
    if store := get_active_store():
        try:
            store.capture_reference_file(
                role, path, rows, captured_at=datetime.now(timezone.utc).isoformat()
            )
        except Exception as ex:  # a reporting failure must not stop a migration
            logger.warning("Could not store reference file %s: %s", path, ex)


def iter_fields(marc_record):
    """Iterate a MARC record's fields, tracking which field (and occurrence) is current.

    Issues located with marc_location during an iteration step get the occurrence of
    that field among the fields with the same tag, so repeated fields are told apart.
    """
    store = get_active_store()
    seen: dict = {}
    try:
        for marc_field in marc_record:
            occurrence = seen.get(marc_field.tag, 0)
            seen[marc_field.tag] = occurrence + 1
            if store:
                store.current_field = (marc_field.tag, occurrence)
            yield marc_field
    finally:
        if store:
            store.current_field = None


def column_location(column: str) -> tuple:
    """Build a (kind, path) location for a column in a delimited file."""
    return "column", column


def capture_row(index: int, source: Any, record: Any, kind: str | None = None):
    """Set the current record for a delimited or JSON row, if a store is active.

    The row is addressable by its zero-based index in the file until a legacy id
    is added with add_current_ids. The payload is serialized only if an issue is logged.
    """
    store = get_active_store()
    if store is None:
        return
    store.set_current_record(
        [str(index), f"row {index}"],
        getattr(source, "name", None) or str(source),
        index,
        "json",
        lambda: json.dumps(record, default=str),
        kind=kind,
    )


def clear_current_record():
    """Forget the current record, if a store is active."""
    store = get_active_store()
    if store is not None:
        store.clear_current_record()


def set_active_store(store: ReportStore | None):
    """Set the store used by MigrationReport and Helper.log_data_issue."""
    global _active_store
    _active_store = store


def get_active_store() -> ReportStore | None:
    """Return the active store, or None if the current run does not persist reports."""
    if _active_store is not None and not _active_store.closed:
        return _active_store
    return None
