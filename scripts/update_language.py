"""Sync a locale's translation file with the keys in the source locale."""

import json
import re
from pathlib import Path

from argparse_prompt import PromptParser

REPO_ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    parser = PromptParser()
    parser.add_argument(
        "--translations-dir",
        help=("Directory to read and write the translations."),
        default=REPO_ROOT / "src" / "folio_migration_tools" / "translations",
        type=Path,
        prompt=False,
    )
    parser.add_argument(
        "--source-lang",
        help=("Locale whose keys are copied to the target."),
        default="en",
        prompt=False,
    )
    parser.add_argument(
        "--target-lang",
        help=("Locale to update, e.g. fr."),
    )
    args = parser.parse_args()

    source_filename = args.translations_dir / f"{args.source_lang}.json"
    target_filename = args.translations_dir / f"{args.target_lang}.json"
    with open(source_filename, encoding="utf-8") as f:
        source_translations = json.load(f)
    if target_filename.exists():
        with open(target_filename, encoding="utf-8") as f:
            target_translations = json.load(f)
    else:
        target_translations = {}

    # Print keys in translation not present in source
    for key in target_translations:
        if key not in source_translations:
            print(
                f"Key '{key}' in target not in source. "
                f"Check if it was renamed, or if it is still needed."
            )
    # Update target translations
    for key in source_translations:
        if key not in target_translations:
            print(f"Adding new key '{key}'")
            if isinstance(source_translations[key], str):
                target_translations[key] = "TRANSLATE ME: " + source_translations[key]
            else:
                target_translations[key] = {}
                for subkey in source_translations[key]:
                    target_translations[key][subkey] = (
                        "TRANSLATE ME: " + source_translations[key][subkey]
                    )
    # Check for missing format
    missing_format_re = re.compile(r"(?<!%)\{[^\}]*\}")
    for key in target_translations:
        if isinstance(target_translations[key], str):
            if missing_format_re.search(target_translations[key]):
                print(f"Key '{key}' may not format correctly: format must have %")
        else:
            for subkey in target_translations[key]:
                if missing_format_re.search(target_translations[key][subkey]):
                    print(
                        f"Key '{key}' plural '{subkey}' may not format "
                        f"correctly: format must have %"
                    )
    # Write
    with open(target_filename, "w", encoding="utf-8") as f:
        json.dump(target_translations, f, sort_keys=True, indent=2, ensure_ascii=False)
