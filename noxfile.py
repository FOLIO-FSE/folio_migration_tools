# noxfile.py
import logging
import os
import sys

import nox

nox.options.sessions = "lint", "safety", "tests", "docs"
locations = "src", "tests", "noxfile.py", "docs/source/conf.py"

if "GITHUB_TOKEN" in os.environ:
    env = {"GITHUB_TOKEN": os.environ["GITHUB_TOKEN"]}
else:
    logging.error("No GITHUB_TOKEN environment variable set nor any .env file exists")
    sys.exit(0)


@nox.session()
def tests(session):
    posargs = ["--cov=./", "--cov-report=xml", "--durations=20"]
    session.run("uv", "sync", "--all-groups", "--all-extras", external=True)
    session.run("uv", "run", "pytest", *posargs, env=env, external=True)


@nox.session()
def lint(session):
    args = session.posargs or locations
    session.run("uv", "run", "ruff", "check", "--fix", *args, external=True)


@nox.session()
def black(session):
    args = session.posargs or locations
    session.run("uv", "sync", "--all-groups", "--all-extras", external=True)
    session.run("uv", "run", "ruff", "format", *args, external=True)


@nox.session()
def safety(session):
    session.run("uvx", "safety", "scan", external=True)


@nox.session()
def docs(session) -> None:
    """Build the documentation."""
    session.run("uv", "sync", "--group", "docs", external=True)
    session.run("uv", "run", "sphinx-build", "docs/source", "docs/_build", external=True)
