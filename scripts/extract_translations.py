# Extract Translations from Source
import argparse
import re
import json
from pathlib import Path

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target-dir",
        help=("Directory to write the translations."),
        default="translations",
    )
    parser.add_argument(
        "--source-dir",
        help=("Directory to pull source files."),
        default="src",
    )
    args = parser.parse_args()

    source_files = Path(args.source_dir).rglob("*.py")

    internationalization_re = re.compile(r"(?<=i18n.t\()\s*([\"'])(.*?[^\\])\1", re.MULTILINE)
    found_keys: set[str] = set()
    for file in source_files:
        with open(file) as f:
            src = f.read()
        found_keys.update(
            [
                x.encode("utf-8").decode("unicode_escape")
                for (_, x) in internationalization_re.findall(src)
            ]
        )

    en_filename = Path(args.target_dir) / "en.json"
    with open(en_filename) as f:
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
            translations[key] = key
    # Check for missing format
    missing_format_re = re.compile(r"(?<!%)\{.*?\}")
    for key in translations:
        if isinstance(translations[key], str):
            if missing_format_re.search(translations[key]):
                print(f"Key '{key}' may not format correctly: format must have %")
        else:
            for subkey in translations[key]:
                if missing_format_re.search(translations[key][subkey]):
                    print(
                        f"Key '{key}' plural '{subkey}' may not format correctly: format must have %"
                    )
    # Write
    with open(en_filename, "w") as f:
        json.dump(translations, f, sort_keys=True, indent=2)
