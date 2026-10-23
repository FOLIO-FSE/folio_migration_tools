"""Render migration reports from a report database.

Reads the SQLite database written by ``report_store`` and produces the same markdown
report a task writes at wrap-up, optionally in another locale or limited to some
sections. The layout comes from the same functions the tasks use, so the default
output is identical.
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import i18n

from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.migration_report import render_report_markdown


@contextmanager
def _locale(locale: str | None):
    """Switch the i18n locale for the duration of a render."""
    original = i18n.get("locale")
    if locale:
        i18n.set("locale", locale)
    try:
        yield
    finally:
        i18n.set("locale", original)


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    """Open a report database for reading only."""
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Report database not found: {path}")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def render_text(translated: int, key: str, params_json: str | None) -> str:
    """Turn a stored measure or message back into text in the active locale."""
    params = json.loads(params_json) if params_json else {}
    if translated == 1:
        return i18n_t(key, **params)
    if translated == 2:
        return "".join(
            i18n_t(part["key"], **(part.get("params") or {})) if "key" in part else part["text"]
            for part in params.get("parts", [])
        )
    return key


def _title(row: sqlite3.Row, title: str | None) -> str:
    if title is not None:
        return title
    data = json.loads(row["report_title_json"]) if row["report_title_json"] else {}
    if "key" in data:
        return i18n_t(data["key"], **data.get("params", {}))
    return data.get("text") or row["task_name"] or ""


def _sections(conn: sqlite3.Connection, run_id: int) -> list:
    """Return [(section_id, [(label, count), ...])] in the order they were written."""
    sections: dict = {}
    for r in conn.execute(
        "SELECT section_id, measure_key, params_json, translated, count FROM report_stat"
        " WHERE run_id = ? ORDER BY rowid",
        (run_id,),
    ):
        label = render_text(r["translated"], r["measure_key"], r["params_json"])
        measures = sections.setdefault(r["section_id"], {})
        # Measures that read the same in this locale are one line, as in the live report
        measures[label] = measures.get(label, 0) + r["count"]
    return [(section_id, list(measures.items())) for section_id, measures in sections.items()]


def _mapping_fields(conn: sqlite3.Connection, run_id: int) -> tuple:
    folio = {}
    legacy = {}
    for r in conn.execute(
        "SELECT side, field_name, present, mapped FROM mapping_stat WHERE run_id = ?", (run_id,)
    ):
        if r["side"] == "folio":
            folio[r["field_name"]] = [r["mapped"]]
        else:
            legacy[r["field_name"]] = [r["present"], r["mapped"]]
    return folio, legacy


def render_markdown(
    db_path: Path,
    locale: str | None = None,
    sections: list[str] | None = None,
    exclude_sections: list[str] | None = None,
    include_mapping: bool = True,
    title: str | None = None,
) -> str:
    """Render the markdown report of a run.

    Args:
        db_path: The report database of a task run.
        locale: Render in this locale. Defaults to the active locale.
        sections: Only include these section ids. Defaults to all.
        exclude_sections: Leave out these section ids.
        include_mapping: Include the mapped FOLIO and legacy field tables, if the
            task recorded them.
        title: Override the report title. Defaults to the stored one.
    """
    conn = connect_readonly(db_path)
    try:
        run = conn.execute("SELECT * FROM run ORDER BY run_id DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError(f"No run recorded in {db_path}")
        started = datetime.fromisoformat(run["started"])
        finished = datetime.fromisoformat(run["finished"] or run["started"])
        folio, legacy = _mapping_fields(conn, run["run_id"])
        with _locale(locale):
            wanted = _sections(conn, run["run_id"])
            if sections is not None:
                wanted = [s for s in wanted if s[0] in sections]
            if exclude_sections:
                wanted = [s for s in wanted if s[0] not in exclude_sections]
            text = render_report_markdown(_title(run, title), started, finished, wanted)
            if include_mapping and (folio or legacy):
                text += Helper.render_mapping_report(
                    run["mapping_total_records"] or 0, folio, legacy
                )
        return text
    finally:
        conn.close()
