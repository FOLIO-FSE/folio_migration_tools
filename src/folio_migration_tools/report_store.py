"""SQLite persistence for migration reports and data issues.

One database is written per task run. Re-running a task on the same iteration
replaces the database, mirroring how the report and log files are overwritten.
"""

import ast
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

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
    translated INTEGER NOT NULL DEFAULT 1,
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
    raw = value if isinstance(value, str) else repr(value) if value is not None else ""
    if value is None or value == "" or value == [] or value == ():
        return RecordRef([], "none", "")
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


class ReportStore:
    """Owns the SQLite connection and buffers writes for one task run."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self._remove_existing()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=OFF")
        self.conn.executescript(SCHEMA)
        self.conn.execute("INSERT INTO schema_info (version) VALUES (?)", (SCHEMA_VERSION,))
        self.conn.commit()
        self.run_id: Optional[int] = None
        self._issue_buffer: list = []

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
        config: Optional[dict] = None,
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
                key = getattr(measure, "key", None)
                params = getattr(measure, "params", None)
                rows.append(
                    (
                        self.run_id,
                        section_id,
                        key or str(measure),
                        json.dumps(params or {}, sort_keys=True, default=str),
                        1 if key else 0,
                        count,
                    )
                )
        self.conn.executemany(
            "INSERT OR REPLACE INTO report_stat (run_id, section_id, measure_key, params_json,"
            " translated, count) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

    def add_issue(
        self,
        issue_type: str,
        record: Any,
        message: Any,
        legacy_value: Any,
        location_kind: Optional[str] = None,
        location_path: Optional[str] = None,
        ts: str = "",
    ):
        ref = normalize_record_ref(record)
        key = getattr(message, "key", None)
        params = getattr(message, "params", None)
        self._issue_buffer.append(
            (
                self.run_id,
                None,
                ref.primary,
                ref.kind,
                ref.raw if ref.raw != ref.primary else None,
                issue_type,
                key or str(message),
                json.dumps(params, sort_keys=True, default=str) if params else None,
                1 if key else 0,
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
        self.flush()
        self.conn.close()
