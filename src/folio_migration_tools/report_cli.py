"""Command line interface for regenerating reports from a report database.

Run as ``folio-migration-tools report --db <file>``. Reads the latest run in the
database and writes the markdown report and/or the data issues TSV, optionally in
other locales or limited to some sections.
"""

import argparse
import json
import sys
from os import environ
from pathlib import Path

import i18n
from pydantic import ValidationError, field_validator

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
        description="Regenerate the migration report and data issues from a report database."
        " The latest run in the database is used.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="JSON file with default values for the options below, e.g."
        ' {"locale": ["en", "fr"], "excludeSections": ["Trivia"], "minCount": 2,'
        ' "mapping": false}. Keys are the option names without the dashes, in snake_case'
        " or camelCase. Options given on the command line take precedence.",
    )
    parser.add_argument("--db", required=True, type=Path, help="Path to the report .sqlite file")
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
        help="Write the markdown report here instead of stdout. With several locales the"
        " locale is added to the file name (report.md -> report_fr.md).",
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
        if args.list_sections:
            for section_id, measures, total in list_sections(args.db, (args.locale or [None])[0]):
                print(f"{section_id}\t{measures} measures\t{total} total")
            return 0
        locales = args.locale or [None]
        several = len(locales) > 1
        if several and not (args.output or args.issues_tsv or args.json_output):
            sys.exit(
                "--output, --issues-tsv or --json-output is required with more than one locale"
            )
        filters = ReportFilters(
            sections=args.sections,
            exclude_sections=args.exclude_sections,
            mapping=not args.no_mapping,
            min_count=args.min_count,
            measure_contains=args.measure_contains,
        )
        for locale in locales:
            if args.output or not (args.issues_tsv or args.json_output):
                text = render_markdown(
                    args.db,
                    locale=locale,
                    filters=filters,
                )
                if args.output:
                    _target(args.output, locale, several).write_text(text, encoding="utf-8")
                else:
                    print(text)
            if args.issues_tsv:
                _target(args.issues_tsv, locale, several).write_text(
                    render_issues_tsv(args.db, locale), encoding="utf-8"
                )
            if args.json_output:
                _target(args.json_output, locale, several).write_text(
                    render_report_json(args.db, locale, filters, args.json_localized),
                    encoding="utf-8",
                )
    except (FileNotFoundError, ValueError) as ex:
        print(ex, file=sys.stderr)
        return 1
    return 0
