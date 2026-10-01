"""Shared pytest fixtures."""

from pathlib import Path

import i18n
import pytest

I18N_CONFIG = Path(__file__).parents[1] / "src/folio_migration_tools/i18n_config.py"


@pytest.fixture
def report_language(request):
    """Load the packaged i18n config and set the report locale for one test.

    Defaults to French. Parametrize with indirect=True to use another locale.
    """
    i18n.load_config(I18N_CONFIG)
    original_locale = i18n.get("locale")
    i18n.set("locale", getattr(request, "param", "fr"))
    yield i18n.get("locale")
    i18n.set("locale", original_locale)
