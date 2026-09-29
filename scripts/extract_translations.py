"""Extract translation keys from source into translations/en.json."""

import argparse
import ast
import json
import re
from pathlib import Path
from typing import TypeGuard

import i18n

REPO_ROOT = Path(__file__).resolve().parents[1]
PLACEHOLDER_RE = re.compile(r"%\{(\w+)\}")
# Keyword arguments consumed by python-i18n itself rather than by placeholders
I18N_KWARGS = {"locale", "default", "count"}


def is_translation_call(node: ast.AST) -> TypeGuard[ast.Call]:
    """Match i18n.t(...) and i18n_t(...) calls."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if isinstance(func, ast.Attribute):
        return func.attr == "t" and isinstance(func.value, ast.Name) and func.value.id == "i18n"
    return isinstance(func, ast.Name) and func.id == "i18n_t"


def unknown_i18n_function(node: ast.AST) -> str | None:
    """Return <name> for i18n.<name>(...) calls where python-i18n has no such function."""
    if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
        return None
    func = node.func
    if (
        isinstance(func.value, ast.Name)
        and func.value.id == "i18n"
        and not hasattr(i18n, func.attr)
    ):
        return func.attr
    return None


def extract_keys(source_files, source_root: Path) -> set[str]:
    found_keys: set[str] = set()
    for file in source_files:
        # The cached wrapper forwards its key argument, so it has no literal keys
        if file.name == "i18n_cache.py":
            continue
        tree = ast.parse(file.read_text(encoding="utf-8"), filename=str(file))
        for node in ast.walk(tree):
            if name := unknown_i18n_function(node):
                print(f"{file.relative_to(source_root)}:{node.lineno}: i18n.{name} does not exist")
            if not is_translation_call(node) or not node.args:
                continue
            location = f"{file.relative_to(source_root)}:{node.lineno}"
            key_node = node.args[0]
            # The parser has already joined implicit concatenation and resolved escapes
            if not (isinstance(key_node, ast.Constant) and isinstance(key_node.value, str)):
                # blurbs.{blurb_id}.title/description lookups are dynamic by design
                if not (
                    isinstance(key_node, ast.JoinedStr)
                    and ast.unparse(key_node).startswith(("f'blurbs.", 'f"blurbs.'))
                ):
                    print(f"{location}: non-literal key {ast.unparse(key_node)!r} not extracted")
                continue
            key = key_node.value
            found_keys.add(key)
            kwargs = {kw.arg for kw in node.keywords if kw.arg}
            has_unpacked_kwargs = any(kw.arg is None for kw in node.keywords)
            placeholders = set(PLACEHOLDER_RE.findall(key))
            if missing := placeholders - kwargs - I18N_KWARGS:
                if not has_unpacked_kwargs:
                    print(f"{location}: key {key!r} missing arguments for {sorted(missing)}")
            if unused := kwargs - placeholders - I18N_KWARGS:
                print(f"{location}: key {key!r} has unused arguments {sorted(unused)}")
    return found_keys


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-dir",
        help=("Directory to write the translations."),
        default=REPO_ROOT / "src" / "folio_migration_tools" / "translations",
        type=Path,
    )
    parser.add_argument(
        "--source-dir",
        help=("Directory to pull source files."),
        default=REPO_ROOT / "src",
        type=Path,
    )
    args = parser.parse_args()

    found_keys = extract_keys(sorted(args.source_dir.rglob("*.py")), args.source_dir)

    en_filename = args.target_dir / "en.json"
    with open(en_filename, encoding="utf-8") as f:
        translations = json.load(f)

    # Print unused translations
    for key in translations:
        if key.startswith("blurbs."):
            continue
        if key not in found_keys:
            print(f"Use of key '{key}' not found. Check if you should delete it.")

    # Load new translations
    for key in found_keys:
        if key not in translations:
            print(f"Adding new key '{key}'")
            translations[key] = key
    # Check for missing format
    missing_format_re = re.compile(r"(?<!%)\{[^\}]*\}")
    for key in translations:
        if isinstance(translations[key], str):
            if missing_format_re.search(translations[key]):
                print(f"Key '{key}' may not format correctly: format must have %")
        else:
            for subkey in translations[key]:
                if missing_format_re.search(translations[key][subkey]):
                    print(
                        f"Key '{key}' plural '{subkey}' may not format "
                        f"correctly: format must have %"
                    )
    # Write
    with open(en_filename, "w", encoding="utf-8") as f:
        json.dump(translations, f, sort_keys=True, indent=2, ensure_ascii=False)
