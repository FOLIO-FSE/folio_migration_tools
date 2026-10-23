"""Command line interface for regenerating reports from a report database.

Run as ``folio-migration-tools report <configuration_path> <task_name>`` to use the report
database of a task, or ``folio-migration-tools report --db <file>`` for a specific file.
Writes the markdown report and/or the data issues TSV and JSON statistics into the
iteration's reports folder, optionally in other locales or limited to some sections.
"""

import argparse
import json
import re
import sys
from os import environ
from pathlib import Path

import i18n
from pydantic import ValidationError, field_validator

from folio_migration_tools.config_file_load import merge_load
from folio_migration_tools.report_renderer import (
    ReportFilters,
    list_sections,
    render_issues_tsv,
    render_markdown,
    render_report_json,
    split_csv,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="folio-migration-tools report",
        description="Regenerate the migration report and data issues from the report database"
        " of a task (give the configuration path and task name) or of a file (--db).",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="JSON file with default values for the options below, e.g."
        ' {"locale": ["en", "fr"], "excludeSections": ["Trivia"], "minCount": 2,'
        ' "mapping": false}. Keys are the option names without the dashes, in snake_case'
        " or camelCase. Options given on the command line take precedence.",
    )
    parser.add_argument(
        "configuration_path",
        nargs="?",
        default=environ.get("FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH"),
        help="Path to the configuration file. Not needed with --db",
    )
    parser.add_argument(
        "task_name",
        nargs="?",
        default=environ.get("FOLIO_MIGRATION_TOOLS_TASK_NAME"),
        help="Name of the task to report on. Not needed with --db",
    )
    parser.add_argument(
        "--base_folder_path",
        default=environ.get("FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH"),
        help="Path to the base folder of the library, as for running a task",
    )
    parser.add_argument(
        "--run",
        help="Which run of the task to report on, as the timestamp in the report database"
        " file name (e.g. 20261023-101500), or 'latest'. If the task has several runs and"
        " this is not given, you are asked to choose.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        help="Path to a report .sqlite file, instead of looking up the task's database",
    )
    parser.add_argument(
        "--locale",
        type=split_csv,
        default=[],
        help="Locale(s) to render in, comma separated. One file is written per locale"
        " when --output is given. Defaults to the configured report language.",
    )
    parser.add_argument("--sections", type=split_csv, help="Only include these section ids")
    parser.add_argument("--exclude-sections", type=split_csv, help="Leave out these section ids")
    parser.add_argument(
        "--no-mapping", action="store_true", help="Leave out the field mapping tables"
    )
    parser.add_argument("--min-count", type=int, default=0, help="Hide measures below this count")
    parser.add_argument(
        "--measure-contains", help="Only show measures whose text contains this (case-insensitive)"
    )
    parser.add_argument(
        "--list-sections", action="store_true", help="List the section ids in the run and exit"
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the markdown report here, or to stdout if '-'. Defaults to"
        " report<timestamp>_<task>_<locale>.md in the reports folder next to the database."
        " With several locales the locale is added to the file name (report.md ->"
        " report_fr.md).",
    )
    parser.add_argument(
        "--issues-tsv",
        type=Path,
        help="Also write the data issues as a tab separated file. Locale suffix as --output.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Also write the statistics as JSON. Locale suffix as --output.",
    )
    parser.add_argument(
        "--json-localized",
        action="store_true",
        help="Write the JSON with labels in the locale ({section: {label: count}}) instead"
        " of locale-neutral keys and params.",
    )
    return parser


class ReportFilterConfig(ReportFilters):
    """The options that can be set in a ``--config`` file."""

    locale: list[str] = []
    output: Path | None = None
    issues_tsv: Path | None = None
    json_output: Path | None = None
    json_localized: bool = False

    @field_validator("locale", mode="before")
    @classmethod
    def _split_locale(cls, value):
        return split_csv(value) if isinstance(value, str) else value


def load_config(path: Path) -> dict:
    """Read a filter config file and return it as argparse defaults.

    Raises:
        ValueError: If the file is not a JSON object or has unknown or mistyped keys.
    """
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError as ex:
        raise ValueError(f"Config file not found: {path}") from ex
    except json.JSONDecodeError as ex:
        raise ValueError(f"Config file {path} is not valid JSON: {ex}") from ex
    if not isinstance(raw, dict):
        raise ValueError(f"Config file {path} must contain a JSON object")
    try:
        config = ReportFilterConfig.model_validate(raw)
    except ValidationError as ex:
        problems = "; ".join(
            f"{'.'.join(str(part) for part in err['loc'])}: {err['msg']}" for err in ex.errors()
        )
        raise ValueError(f"Invalid config file {path}: {problems}") from ex
    defaults = config.model_dump(exclude_unset=True)
    if "mapping" in defaults:
        defaults["no_mapping"] = not defaults.pop("mapping")
    return defaults


_DB_NAME = re.compile(r"^migration_(?:(?P<run>\d{8}-\d{6})_)?(?P<task>.+)\.sqlite$")


def find_databases(raw_folder: Path, task_name: str) -> list[tuple[str, Path]]:
    """Return [(run timestamp, path)] of the report databases of a task, oldest first.

    The timestamp is empty for databases written without time stamps in the file names.
    """
    found = []
    for path in raw_folder.glob("migration_*.sqlite"):
        match = _DB_NAME.match(path.name)
        if match and match["task"] == task_name:
            found.append((match["run"] or "", path))
    return sorted(found)


def _choose_run(task_name: str, databases: list[tuple[str, Path]]) -> Path:
    if not sys.stdin.isatty():
        runs = ", ".join(run or path.name for run, path in databases)
        raise ValueError(
            f"{task_name} has several runs: {runs}. Use --run <timestamp> or --run latest"
        )
    print(f"{task_name} has {len(databases)} runs:", file=sys.stderr)
    for number, (run, path) in enumerate(databases, 1):
        print(f"  {number}. {run or path.name}", file=sys.stderr)
    while True:
        answer = input(f"Which run? [1-{len(databases)}, a timestamp, or Enter for latest] ")
        answer = answer.strip()
        if answer in ("", "latest"):
            return databases[-1][1]
        if answer.isdigit() and 1 <= int(answer) <= len(databases):
            return databases[int(answer) - 1][1]
        for run, path in databases:
            if answer == run:
                return path
        print(f"Not a run: {answer}", file=sys.stderr)


def resolve_db(args: argparse.Namespace) -> Path:
    """Find the report database to use from --db or the configuration and task name.

    Raises:
        ValueError: If the task or its database cannot be found, or the run is ambiguous.
    """
    if args.db:
        if args.run:
            raise ValueError("--run cannot be used with --db")
        return args.db
    if not (args.configuration_path and args.task_name):
        raise ValueError("Give a configuration path and task name, or --db")
    try:
        config = merge_load(args.configuration_path)
    except FileNotFoundError as ex:
        raise ValueError(f"Configuration file not found: {args.configuration_path}") from ex
    tasks = [t.get("name", "") for t in config.get("migrationTasks", [])]
    if args.task_name not in tasks:
        raise ValueError(
            f"Task name {args.task_name} not found in the configuration file."
            f" Use one of: {', '.join(tasks)}"
        )
    library = config.get("libraryInformation", {})
    base_folder = args.base_folder_path or library.get("baseFolder")
    iteration = library.get("iterationIdentifier")
    if not base_folder or not iteration:
        raise ValueError(
            "The base folder and iteration identifier are needed to find the report"
            " database. Give --base_folder_path and set iterationIdentifier in the configuration"
        )
    raw_folder = Path(base_folder) / "iterations" / iteration / "reports" / ".raw"
    databases = find_databases(raw_folder, args.task_name)
    if not databases:
        raise ValueError(f"No report database for {args.task_name} found in {raw_folder}")
    if args.run and args.run != "latest":
        for run, path in databases:
            if args.run == run:
                return path
        runs = ", ".join(run or path.name for run, path in databases)
        raise ValueError(f"No run {args.run} for {args.task_name}. Available: {runs}")
    if args.run == "latest" or len(databases) == 1:
        return databases[-1][1]
    return _choose_run(args.task_name, databases)


def _default_output(db: Path, locale: str | None) -> Path:
    """The markdown report path in the reports folder that holds the database."""
    reports_folder = db.parent.parent if db.parent.name == ".raw" else db.parent
    stem = db.stem.removeprefix("migration")
    return reports_folder / f"report{stem}_{locale or i18n.get('locale') or 'en'}.md"


def _load_i18n():
    base = environ.get("FOLIO_MIGRATION_TOOLS_I18N_CONFIG_BASE_PATH")
    try:
        if not base:
            raise i18n.I18nFileLoadError("no base path")
        i18n.load_config(Path(base) / "i18n_config.py")
    except i18n.I18nFileLoadError:
        i18n.load_config(Path(__file__).parent / "i18n_config.py")
    i18n.set("locale", environ.get("FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE", "en"))


def _target(path: Path, locale: str | None, several: bool) -> Path:
    if not several or not locale:
        return path
    return path.with_name(f"{path.stem}_{locale}{path.suffix}")


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _load_i18n()
    try:
        if args.config:
            parser.set_defaults(**load_config(args.config))
            args = parser.parse_args(argv)
        db = resolve_db(args)
        if args.list_sections:
            for section_id, measures, total in list_sections(db, (args.locale or [None])[0]):
                print(f"{section_id}\t{measures} measures\t{total} total")
            return 0
        locales = args.locale or [None]
        several = len(locales) > 1
        to_stdout = str(args.output) == "-"
        if several and to_stdout:
            sys.exit("--output - cannot be used with more than one locale")
        write_markdown = bool(args.output) or not (args.issues_tsv or args.json_output)
        filters = ReportFilters(
            sections=args.sections,
            exclude_sections=args.exclude_sections,
            mapping=not args.no_mapping,
            min_count=args.min_count,
            measure_contains=args.measure_contains,
        )
        for locale in locales:
            if write_markdown:
                text = render_markdown(db, locale=locale, filters=filters)
                if to_stdout:
                    print(text)
                else:
                    target = (
                        _target(args.output, locale, several)
                        if args.output
                        else _default_output(db, locale)
                    )
                    target.write_text(text, encoding="utf-8")
                    print(f"Wrote {target}", file=sys.stderr)
            if args.issues_tsv:
                _target(args.issues_tsv, locale, several).write_text(
                    render_issues_tsv(db, locale), encoding="utf-8"
                )
            if args.json_output:
                _target(args.json_output, locale, several).write_text(
                    render_report_json(db, locale, filters, args.json_localized),
                    encoding="utf-8",
                )
    except (FileNotFoundError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 1
    return 0
