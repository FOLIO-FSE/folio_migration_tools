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
from typing import Annotated

import humps
import i18n
from pydantic import BaseModel, ConfigDict, Field, StrictBool, field_validator

from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.migration_report import render_report_markdown


@contextmanager
def locale_context(locale: str | None):
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


def report_title(row: sqlite3.Row, title: str | None) -> str:
    if title is not None:
        return title
    data = json.loads(row["report_title_json"]) if row["report_title_json"] else {}
    if "key" in data:
        return i18n_t(data["key"], **data.get("params", {}))
    return data.get("text") or row["task_name"] or ""


def load_sections(conn: sqlite3.Connection, run_id: int) -> list:
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


def mapping_fields(conn: sqlite3.Connection, run_id: int) -> tuple:
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


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


class ReportFilters(BaseModel):
    """Which parts of a report to render.

    Keys can be written in snake_case or camelCase. Unknown keys are rejected.
    """

    model_config = ConfigDict(
        extra="forbid", alias_generator=humps.camelize, populate_by_name=True
    )

    sections: list[str] | None = None
    exclude_sections: list[str] | None = None
    mapping: StrictBool = True
    min_count: Annotated[int, Field(ge=0, strict=True)] = 0
    measure_contains: str | None = None

    @field_validator("sections", "exclude_sections", mode="before")
    @classmethod
    def _split_csv(cls, value):
        return split_csv(value) if isinstance(value, str) else value


def filter_sections(sections: list, filters: ReportFilters) -> list:
    """Apply the section and measure filters to [(section_id, [(label, count)])].

    Raises:
        ValueError: If a section id is not in the run.
    """
    known = [section_id for section_id, _ in sections]
    requested = (filters.sections or []) + (filters.exclude_sections or [])
    unknown = [s for s in requested if s not in known]
    if unknown:
        raise ValueError(
            f"Unknown section(s): {', '.join(unknown)}. Available: {', '.join(known)}"
        )
    wanted = sections
    if filters.sections is not None:
        wanted = [s for s in wanted if s[0] in filters.sections]
    if filters.exclude_sections:
        wanted = [s for s in wanted if s[0] not in filters.exclude_sections]
    if filters.min_count or filters.measure_contains:
        needle = (filters.measure_contains or "").lower()
        wanted = [
            (section_id, kept)
            for section_id, measures in wanted
            if (
                kept := [
                    (label, count)
                    for label, count in measures
                    if count >= filters.min_count and needle in str(label).lower()
                ]
            )
        ]
    return wanted


def render_markdown(
    db_path: Path,
    locale: str | None = None,
    filters: ReportFilters | None = None,
    title: str | None = None,
) -> str:
    """Render the markdown report of a run.

    Args:
        db_path: The report database of a task run.
        locale: Render in this locale. Defaults to the active locale.
        filters: Which sections and measures to include. Defaults to everything,
            including the mapped FOLIO and legacy field tables if the task recorded them.
        title: Override the report title. Defaults to the stored one.

    Raises:
        ValueError: If there is no run, or a section id is not in the run.
    """
    filters = filters or ReportFilters()
    conn = connect_readonly(db_path)
    try:
        run = conn.execute("SELECT * FROM run ORDER BY run_id DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError(f"No run recorded in {db_path}")
        started = datetime.fromisoformat(run["started"])
        finished = datetime.fromisoformat(run["finished"] or run["started"])
        folio, legacy = mapping_fields(conn, run["run_id"])
        with locale_context(locale):
            wanted = filter_sections(load_sections(conn, run["run_id"]), filters)
            text = render_report_markdown(report_title(run, title), started, finished, wanted)
            if filters.mapping and (folio or legacy):
                text += Helper.render_mapping_report(
                    run["mapping_total_records"] or 0, folio, legacy
                )
        return text
    finally:
        conn.close()


def _measure_json(row: sqlite3.Row) -> dict:
    params = json.loads(row["params_json"]) if row["params_json"] else {}
    kind = {0: "literal", 1: "key", 2: "composed"}[row["translated"]]
    return {"kind": kind, "key": row["measure_key"], "params": params, "count": row["count"]}


def render_report_json(
    db_path: Path,
    locale: str | None = None,
    filters: ReportFilters | None = None,
    localized: bool = False,
) -> str:
    """Render the statistics of the latest run as JSON.

    By default the output is locale-neutral: every measure is a ``{kind, key, params,
    count}`` object, where ``kind`` is ``key`` (a translation key with its params),
    ``literal`` (text that was never translated) or ``composed`` (``params.parts`` hold
    keys and text to join). With ``localized`` the output has the shape of the JSON a
    task writes, ``{section: {label: count}}``, with labels in ``locale``. The filters
    apply to both. The mapping field tables are included unless ``filters.mapping`` is
    false.

    Raises:
        ValueError: If there is no run, or a section id is not in the run.
    """
    filters = filters or ReportFilters()
    conn = connect_readonly(db_path)
    try:
        run = conn.execute("SELECT * FROM run ORDER BY run_id DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError(f"No run recorded in {db_path}")
        with locale_context(locale):
            wanted = filter_sections(load_sections(conn, run["run_id"]), filters)
            if localized:
                data = {section_id: dict(measures) for section_id, measures in wanted}
                return json.dumps(data, indent=2, ensure_ascii=False)
            kept = {
                section_id: {label for label, _ in measures} for section_id, measures in wanted
            }
            sections: dict = {section_id: [] for section_id in kept}
            for r in conn.execute(
                "SELECT section_id, measure_key, params_json, translated, count FROM report_stat"
                " WHERE run_id = ? ORDER BY rowid",
                (run["run_id"],),
            ):
                label = render_text(r["translated"], r["measure_key"], r["params_json"])
                if r["section_id"] in kept and label in kept[r["section_id"]]:
                    sections[r["section_id"]].append(_measure_json(r))
        data = {
            "run": {
                "task_name": run["task_name"],
                "task_type": run["task_type"],
                "started": run["started"],
                "finished": run["finished"],
                "tool_version": run["tool_version"],
                "total_records": run["mapping_total_records"],
            },
            "sections": sections,
        }
        if filters.mapping:
            folio, legacy = mapping_fields(conn, run["run_id"])
            if folio or legacy:
                data["mapping"] = {"folio": folio, "legacy": legacy}
        return json.dumps(data, indent=2, ensure_ascii=False)
    finally:
        conn.close()


def list_sections(db_path: Path, locale: str | None = None) -> list[tuple[str, int, int]]:
    """Return [(section_id, number of measures, total count)] of the latest run."""
    conn = connect_readonly(db_path)
    try:
        run = conn.execute("SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError(f"No run recorded in {db_path}")
        with locale_context(locale):
            sections = load_sections(conn, run["run_id"])
        return [
            (section_id, len(measures), sum(count for _, count in measures))
            for section_id, measures in sections
        ]
    finally:
        conn.close()


def render_issues_tsv(db_path: Path, locale: str | None = None) -> str:
    """Render the data issues of the latest run as the tab separated log.

    Columns: issue type, record id, message, offending value. The message is in the
    requested locale. Record ids are the normalized ids, not list representations.
    """
    conn = connect_readonly(db_path)
    try:
        run = conn.execute("SELECT run_id FROM run ORDER BY run_id DESC LIMIT 1").fetchone()
        if run is None:
            raise ValueError(f"No run recorded in {db_path}")
        lines = []
        with locale_context(locale):
            for r in conn.execute(
                "SELECT issue_type, record_id, record_id_raw, message_key, params_json,"
                " translated, legacy_value FROM data_issue WHERE run_id = ? ORDER BY id",
                (run["run_id"],),
            ):
                message = render_text(r["translated"], r["message_key"] or "", r["params_json"])
                record_id = r["record_id"] or r["record_id_raw"] or ""
                lines.append(
                    f"{r['issue_type']}\t{record_id}\t{message}\t{r['legacy_value'] or ''}\n"
                )
        return "".join(lines)
    finally:
        conn.close()
