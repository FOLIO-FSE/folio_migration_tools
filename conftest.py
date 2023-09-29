from pathlib import Path
import pytest


def pytest_addoption(parser):
    parser.addoption("--password", action="store")
    parser.addoption("--tenant_id", action="store")
    parser.addoption("--okapi_url", action="store")
    parser.addoption("--username", action="store")
    # parser.addoption("--folio_release", action="store")


i18n_config_file = Path(__file__).parent / "i18n_config.py"


@pytest.fixture(scope="session", autouse=True)
def install_l10n():
    import i18n

    i18n.load_config(i18n_config_file)
