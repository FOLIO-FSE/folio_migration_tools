# Update Language Files with new Keys

from argparse_prompt import PromptParser
import re
import json
from pathlib import Path

if __name__ == "__main__":
    parser = PromptParser()
    parser.add_argument(
        "--translations-dir",
        help=("Directory to read and write the translations."),
        default="translations",
        prompt=False,
    )
    parser.add_argument(
        "--source-lang",
        help=("Directory to pull source files."),
        default="en",
        prompt=False,
    )
    parser.add_argument(
        "--target-lang",
        help=("Target language to convert to."),
    )
    args = parser.parse_args()

    source_filename = Path(args.translations_dir) / f"{args.source_lang}.json"
    target_filename = Path(args.translations_dir) / f"{args.target_lang}.json"
    with open(source_filename) as f:
        source_translations = json.load(f)
    if target_filename.exists():
        with open(target_filename) as f:
            target_translations = json.load(f)
    else:
        target_translations = {}

    # Print keys in translation not present in source
    for key in target_translations:
        if key not in source_translations:
            print(
                f"Key '{key}' in target not in source. Check if it was renamed, or if it is still needed."
            )
    # Update target translations
    for key in source_translations:
        if key not in target_translations:
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
                        f"Key '{key}' plural '{subkey}' may not format correctly: format must have %"
                    )
    # Write
    with open(target_filename, "w") as f:
        json.dump(target_translations, f, sort_keys=True, indent=2)
