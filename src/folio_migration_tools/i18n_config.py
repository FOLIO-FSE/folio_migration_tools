"""Internationalization configuration.

Defines settings for the i18n library used for translation of user-facing messages
in migration reports and logs. Configures translation file locations and formats.
"""

from pathlib import Path

settings = {
    "file_format": "json",
    "skip_locale_root_data": True,
    "fallback": "en",
    "filename_format": "{locale}.{format}",
    "load_path": [Path(__file__).parent / "translations"],
}
