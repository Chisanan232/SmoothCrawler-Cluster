# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.

import os
import sys
# sys.path.insert(0, os.path.abspath('.'))
sys.path.insert(0, os.path.abspath('../../'))


# -- Project information -----------------------------------------------------
from smoothcrawler_cluster.__pkg_info__ import __title__, __author__, __version__

project = __title__
copyright = '2022, Liu, Bryant'
author = __author__

# The full version, including alpha/beta/rc tags
release = __version__


# -- General configuration ---------------------------------------------------
# The master toctree document.
master_doc = 'index'

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

# The suffix(es) of source filenames.
# You can specify multiple suffix as a list of string:
#
# source_suffix = ['.rst', '.md']
source_suffix = ".rst"

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# The language for content autogenerated by Sphinx. Refer to documentation
# for a list of supported languages.
language = 'en'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
# exclude_patterns = []
exclude_patterns = ['build']

# The name of the Pygments (syntax highlighting) style to use.
pygments_style = 'sphinx'


# -- Options for autodoc -------------------------------------------------

# Let the order of functions which be generated by plugin 'autodoc' to follow the
# order of source code.
autodoc_member_order = 'bysource'


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
# html_theme = 'alabaster'    # Default theme
# html_theme = 'sphinx_rtd_theme'
html_theme = 'furo'
html_show_sphinx = False
globaltoc_maxdepth = 3


def setup(app):
    app.add_css_file('css/custom.css')  # may also be an URL


# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
html_css_files = ['css/custom.css']


# Custom sidebar templates, must be a dictionary that maps document names
# to template names.
#
# This is required for the alabaster theme
# refs: http://alabaster.readthedocs.io/en/latest/installation.html#sidebars
html_sidebars = {
    '**': [
        # 'globaltoc.html',
        # # 'relations.html',  # needs 'show_related': True theme option to display
        # 'searchbox.html'

        "sidebar/scroll-start.html",
        "sidebar/brand.html",
        "sidebar/search.html",
        "sidebar/navigation.html",
        "sidebar/ethical-ads.html",
        "sidebar/scroll-end.html",
    ],
    'using/windows': [
        'windowssidebar.html',
        'searchbox.html'
    ],
}

# -- Options for HTMLHelp output ------------------------------------------

# Output file base name for HTML help builder.
htmlhelp_basename = 'smoothcrawlerclusterdoc'


# -- Options for LaTeX output ---------------------------------------------

latex_elements = {
    # The paper size ('letterpaper' or 'a4paper').
    #
    'papersize': 'a4paper',

    # The font size ('10pt', '11pt' or '12pt').
    #
    'pointsize': '11pt',

    # Additional stuff for the LaTeX preamble.
    #
    # 'preamble': '',

    # Latex figure (float) alignment
    #
    # 'figure_align': 'htbp',
}

# Grouping the document tree into LaTeX files. List of tuples
# (source start file, target name, title,
#  author, documentclass [howto, manual, or own class]).
latex_documents = [
  ('index', 'smoothcrawler_cluster.tex', 'SmoothCrawler-Cluster Documentation',
   'Bryant, Liu and the SmoothCrawler-Cluster community', 'manual'),
]


# -- Options for manual page output ---------------------------------------

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    (master_doc, 'smoothcrawler_cluster', 'SmoothCrawler-Cluster Documentation',
     ['Bryant, Liu and the SmoothCrawler-Cluster community'], 1)
]

# pylint: disable-next=consider-using-namedtuple-or-dataclass
intersphinx_mapping = {
    "astroid": ("https://smoothcrawler-cluster.python.org/en/latest/", None),
    "python": ("https://docs.python.org/3", None),
}

# Prevent label issues due to colliding section names
# through including multiple documents
autosectionlabel_prefix_document = True

# Permit duplicated titles in the resulting document.
# See https://github.com/PyCQA/pylint/issues/7362#issuecomment-1256932866
autosectionlabel_maxdepth = 2