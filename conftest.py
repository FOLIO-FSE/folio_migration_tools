import pytest


def pytest_addoption(parser):
    parser.addoption("--password", action="store")
    parser.addoption("--tenant_id", action="store")
    parser.addoption("--okapi_url", action="store")
    parser.addoption("--username", action="store")
    # parser.addoption("--folio_release", action="store")
