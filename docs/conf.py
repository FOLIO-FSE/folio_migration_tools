# docs/conf.py
"""Sphinx configuration."""
project = "FSE FOLIO Migration Tools"
author = "Theodor Tolstoy"
copyright = f"2022, {author}"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "myst_parser",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}
