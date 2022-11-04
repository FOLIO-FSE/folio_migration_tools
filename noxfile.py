# noxfile.py
import logging
import os
import tempfile

import nox

nox.options.sessions = "lint", "safety", "tests", "docs"
locations = "src", "tests", "noxfile.py", "docs/conf.py"

try:
    with open(".env") as f:
        lines = f.readlines()
        token = lines[0].replace("GITHUB_TOKEN=", "")
        env = {"GITHUB_TOKEN": token}
except Exception:
    if "GITHUB_TOKEN" in os.environ:
        env = {"GITHUB_TOKEN": os.environ["GITHUB_TOKEN"]}
    else:
        logging.error("No GITHUB_TOKEN environment variable set nor any .env file exists")
        env = {}


@nox.session()
def tests(session):
    print(session.posargs)
    posargs = [
        "--okapi_url",
        session.posargs[0],
        "--tenant_id",
        session.posargs[1],
        "--username",
        session.posargs[2],
        "--password",
        session.posargs[3],
        "--cov=./",
        "--cov-report=xml",
    ]
    session.run("poetry", "install", external=True)
    session.run("pytest", "--cov", *posargs, env=env)


@nox.session()
def lint(session):
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-annotations",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-docstrings",
        "flake8-import-order",
        "darglint",
    )
    session.run("flake8", *args)


@nox.session()
def black(session):
    args = session.posargs or locations
    session.install("black")
    session.run("black", *args)


@nox.session()
def safety(session):
    with tempfile.NamedTemporaryFile() as requirements:
        session.run(
            "poetry",
            "export",
            "--dev",
            "--format=requirements.txt",
            "--without-hashes",
            f"--output={requirements.name}",
            external=True,
        )
        session.install("safety")
        session.run("safety", "check", f"--file={requirements.name}", "--full-report")


@nox.session()
def docs(session) -> None:
    """Build the documentation."""
    session.run("poetry", "install", "--no-dev", external=True)
    session.install("sphinx")
    session.install("sphinx-autodoc-typehints")
    session.install("myst_parser")
    session.run("sphinx-build", "docs", "docs/_build")
