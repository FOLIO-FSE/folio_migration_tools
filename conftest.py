def pytest_addoption(parser):
    parser.addoption("--password", action="store")
    parser.addoption("--tenant_id", action="store")
    parser.addoption("--okapi_url", action="store")
    parser.addoption("--username", action="store")
    # parser.addoption("--folio_release", action="store")


@pytest.fixture(scope="session", autouse=True)
def install_l10n():
    import i18n

    i18n.set("file_format", "json")
    i18n.set("skip_locale_root_data", True)
    i18n.set("fallback", "en")
    i18n.set("filename_format", "{locale}.{format}")
