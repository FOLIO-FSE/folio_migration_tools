"""Consistency checks for the translation files and localized report rendering."""

import ast
import importlib.util
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import i18n
import pytest

from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.migration_report import MigrationReport

REPO_ROOT = Path(__file__).parents[1]
SOURCE_DIR = REPO_ROOT / "src"
TRANSLATIONS_DIR = SOURCE_DIR / "folio_migration_tools" / "translations"
SOURCE_LOCALE = "en"
TARGET_LOCALES = ["fr"]
PLACEHOLDER_RE = re.compile(r"%\{(\w+)\}")
# update_language.py prefixes new keys with this until someone translates them
UNTRANSLATED_MARKER = "TRANSLATE ME: "
PLURAL_COUNTS = {"zero": 0, "one": 1, "few": 3, "many": 5}


def load_translations(locale: str) -> dict:
    with open(TRANSLATIONS_DIR / f"{locale}.json", encoding="utf-8") as f:
        return json.load(f)


def iter_values(translations: dict):
    """Yield (key, plural form or None, text) for every translation."""
    for key, value in translations.items():
        if isinstance(value, dict):
            for form, text in value.items():
                yield key, form, text
        else:
            yield key, None, value


def placeholders(text: str) -> set[str]:
    return set(PLACEHOLDER_RE.findall(text))


def load_extractor():
    spec = importlib.util.spec_from_file_location(
        "extract_translations", REPO_ROOT / "scripts" / "extract_translations.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def literal_str(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def report_section_ids() -> set[str]:
    """Find the literal section ids passed to MigrationReport.add/set and RefDataMapping."""
    section_ids = set()
    for file in SOURCE_DIR.rglob("*.py"):
        for node in ast.walk(ast.parse(file.read_text(encoding="utf-8"))):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr in ("add", "set")
                and ast.unparse(func.value).endswith("migration_report")
                and node.args
            ):
                candidates = [node.args[0]]
            elif isinstance(func, ast.Name) and func.id == "RefDataMapping":
                # blurb_id is the last parameter
                candidates = [kw.value for kw in node.keywords if kw.arg == "blurb_id"]
                candidates += node.args[-1:]
            else:
                continue
            section_ids.update(s for c in candidates if (s := literal_str(c)))
    return section_ids


@pytest.mark.parametrize("locale", TARGET_LOCALES)
def test_locales_have_the_same_keys(locale):
    source = load_translations(SOURCE_LOCALE)
    target = load_translations(locale)
    assert sorted(set(source) - set(target)) == [], f"missing from {locale}.json"
    assert sorted(set(target) - set(source)) == [], f"not in {SOURCE_LOCALE}.json"


@pytest.mark.parametrize("locale", TARGET_LOCALES)
def test_locales_have_the_same_placeholders(locale):
    source_translations = load_translations(SOURCE_LOCALE)
    source = {(key, form): text for key, form, text in iter_values(source_translations)}
    mismatches = [
        key if form is None else f"{key} ({form})"
        for key, form, text in iter_values(load_translations(locale))
        if (key, form) in source and placeholders(text) != placeholders(source[(key, form)])
    ]
    assert mismatches == []


@pytest.mark.parametrize("locale", TARGET_LOCALES)
def test_locales_have_the_same_plural_forms(locale):
    source = load_translations(SOURCE_LOCALE)
    mismatches = [
        key
        for key, value in load_translations(locale).items()
        if isinstance(value, dict) != isinstance(source.get(key), dict)
        or (isinstance(value, dict) and set(value) != set(source[key]))
    ]
    assert mismatches == []


@pytest.mark.parametrize("locale", TARGET_LOCALES)
def test_no_untranslated_markers(locale):
    untranslated = [
        key
        for key, _, text in iter_values(load_translations(locale))
        if text.startswith(UNTRANSLATED_MARKER)
    ]
    assert untranslated == []


@pytest.mark.parametrize("locale", [SOURCE_LOCALE, *TARGET_LOCALES])
def test_every_translation_resolves(report_language, locale):
    """python-i18n splits keys on dots, so check each key actually resolves."""
    unresolved = []
    for key, form, text in iter_values(load_translations(locale)):
        values: dict[str, str | int] = dict.fromkeys(placeholders(text), "X")
        if form is not None:
            values["count"] = PLURAL_COUNTS[form]
        expected = PLACEHOLDER_RE.sub(lambda m, values=values: str(values[m[1]]), text)
        if i18n.t(key, locale=locale, **values) != expected:
            unresolved.append(key if form is None else f"{key} ({form})")
    assert unresolved == []


def test_source_keys_are_in_translation_files():
    extractor = load_extractor()
    found_keys = extractor.extract_keys(sorted(SOURCE_DIR.rglob("*.py")), SOURCE_DIR)
    for locale in [SOURCE_LOCALE, *TARGET_LOCALES]:
        assert sorted(found_keys - set(load_translations(locale))) == [], locale


def test_report_sections_have_blurbs():
    section_ids = report_section_ids()
    assert "GeneralStatistics" in section_ids
    for locale in [SOURCE_LOCALE, *TARGET_LOCALES]:
        translations = load_translations(locale)
        missing = [
            f"blurbs.{section_id}.{part}"
            for section_id in sorted(section_ids)
            for part in ("title", "description")
            if f"blurbs.{section_id}.{part}" not in translations
        ]
        assert missing == [], locale


def test_french_report_renders_without_raw_keys(report_language):
    report = MigrationReport()
    for section_id in sorted(report_section_ids()):
        report.add(section_id, i18n_t("Measure"))
    report.set("Details", i18n_t("Encoding errors"), 3)
    report.add_general_statistics(i18n_t("Field Mapping Errors found"))
    output = io.StringIO()

    report.write_migration_report(
        i18n_t("Bibliographic records transformation report"),
        output,
        datetime.now(timezone.utc),
    )

    rendered = output.getvalue()
    assert "blurbs." not in rendered
    assert "%{" not in rendered
    assert "## " + i18n_t("Timings", locale="fr") in rendered
    assert i18n_t("Measure", locale="fr") != i18n_t("Measure", locale="en")
