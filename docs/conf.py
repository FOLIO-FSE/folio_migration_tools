# docs/conf.py
"""Sphinx configuration."""
project = "FSE FOLIO Migration Tools"
author = "Theodor Tolstoy"
copyright = f"2022, {author}"
extensions = ["myst_parser", "sphinx.ext.autodoc"]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}
