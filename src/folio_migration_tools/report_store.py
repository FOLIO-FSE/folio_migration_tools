"""SQLite persistence for migration reports and data issues.

One database is written per task run. Re-running a task on the same iteration
replaces the database, mirroring how the report and log files are overwritten.
"""

import ast
import hashlib
import json
import logging
import sqlite3
import weakref
from dataclasses import dataclass, field
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
    config_json TEXT
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
                json.dumps(config, default=str) if config is not None else None,
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
        self.conn.executemany(
            "INSERT INTO report_stat (run_id, section_id, measure_key, params_json,"
            " translated, count) VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT (run_id, section_id, measure_key, params_json)"
            " DO UPDATE SET count = count + excluded.count",
            rows,
        )
        self.conn.commit()

    def set_current_record(
        self,
        record_id: Any,
        source_file: str = "",
        source_index: int = 0,
        format: str = "",
        payload_getter: Callable[[], Any] | None = None,
    ):
        """Mark the record being processed. Nothing is written until an issue is logged.

        payload_getter is called lazily, once, when the first issue for this record
        is added, so clean records cost only this pointer assignment.
        """
        self._current = {
            "ref": normalize_record_ref(record_id),
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
            payload = cur["getter"]() if cur["getter"] else None
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            ids = sorted(cur["ref"].ids)
            row = self.conn.execute(
                "INSERT INTO legacy_record (run_id, record_id, record_id_kind, source_file,"
                " source_index, format, payload, payload_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
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
            if set(ref.ids) & current_ids:
                record_pk = self._current_record_pk()
        if len(ref.ids) > 1:
            ref = RecordRef(sorted(ref.ids), ref.kind, ref.raw)
        text, params_json, translated = describe_text(message)
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


def marc_location(tag: str, subfield: str | None = None, occurrence: int | None = None) -> tuple:
    """Build a (kind, path) location for a MARC field or subfield.

    The path grammar is TAG[OCCURRENCE]$CODE. The occurrence (zero-based index among
    fields with the same tag) and the subfield code are optional: 245, 245$a, 245[0]$a.
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
    return "marc", path


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


def capture_row(index: int, source: Any, record: Any):
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
    )


def set_active_store(store: ReportStore | None):
    """Set the store used by MigrationReport and Helper.log_data_issue."""
    global _active_store
    _active_store = store


def get_active_store() -> ReportStore | None:
    """Return the active store, or None if the current run does not persist reports."""
    if _active_store is not None and not _active_store.closed:
        return _active_store
    return None
