# docs/conf.py
import datetime

"""Sphinx configuration."""
project = "FSE FOLIO Migration Tools"
author = "EBSCO Information Services"
copyright = f"{datetime.date.today().year}, {author}"
extensions = ["myst_parser"]
html_theme = "sphinx_book_theme"
myst_heading_anchors = 3
html_theme_options = {
    "repository_url": "https://github.com/folio-fse/folio_migration_tools",
    "use_repository_button": True,
    "show_navbar_depth": 2,
    "max_navbar_depth": 3,
    "show_nav_level": 2,
    "collapse_navigation": True
}
myst_enable_extensions = [
    "deflist",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".txt": "markdown",
    ".md": "markdown",
}
