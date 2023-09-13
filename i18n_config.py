from pathlib import Path

settings = {
    "file_format": "json",
    "skip_locale_root_data": True,
    "fallback": "en",
    "filename_format": "{locale}.{format}",
    "load_path": [Path(__file__).parent / "translations"],
}
