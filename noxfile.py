# noxfile.py
import json
import tempfile
import nox

nox.options.sessions = "lint", "safety", "tests"
locations = "src", "tests", "noxfile.py"

with open(".env.json") as f:
    env_file = json.load(f)


@nox.session()
def tests(session):
    posargs = [
        "--password",
        env_file["password"],
        "--tenant_id",
        env_file["tenant_id"],
        "--okapi_url",
        env_file["okapi_url"],
        "--username",
        env_file["username"],
    ]
    session.run("poetry", "install", "--only", "main", external=True)
    session.run("pytest", "--cov", *posargs, env=env_file)


@nox.session()
def lint(session):
    args = session.posargs or locations
    session.install(
        "flake8",
        "flake8-bandit",
        "flake8-black",
        "flake8-bugbear",
        "flake8-import-order",
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
