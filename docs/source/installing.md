# Installing

## Python Version Requirements
Make sure you are running Python 3.10 or above (3.12 recommended). You can check your Python version with:

```shell
python -V  # outputs the version of Python
```

uv can automatically manage Python versions for you - see the [uv Python version documentation](https://docs.astral.sh/uv/concepts/python-versions/).

## Installing with uv (Recommended)

[uv](https://docs.astral.sh/uv/) is a fast Python package and project manager. It handles virtual environments and Python versions automatically.

### Install uv
```shell
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Install folio_migration_tools
```shell
uv tool install folio_migration_tools
```

Test the installation:
```shell
folio-migration-tools -h
```

## Installing with pip

If you prefer pip, we recommend using a virtual environment:

```shell
python -m venv ./.venv       # Creates a virtual env in the current folder
source .venv/bin/activate    # Activates the venv (macOS/Linux)
# or on Windows: .venv\Scripts\activate

pip install folio_migration_tools
```

Test the installation:
```shell
folio-migration-tools -h
```
