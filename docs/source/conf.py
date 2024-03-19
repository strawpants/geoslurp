# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information
from datetime import datetime
import os

project = 'geoslurp'
copyright = str(datetime.now().year)+', Roelof Rietbroek'
author = 'Roelof Rietbroek'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration


extensions = ['nbsphinx', 'sphinxcontrib.apidoc', 'sphinx.ext.autodoc','sphinx.ext.napoleon','sphinxcontrib.autoprogram']

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store','**.ipynb_checkpoints']

apidoc_template_dir='_templates'
#figure out the actual installation directory
import geoslurp
apidoc_module_dir=os.path.dirname(geoslurp.__file__)

apidoc_output_dir = 'references'
apidoc_separate_modules = False
apidoc_module_first=True
apidoc_toc_file=False

autodoc_mock_imports = ["cdsapi","motu_utils"]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output
html_theme = 'sphinx_rtd_theme'
html_static_path = ['_static']

napoleon_numpy_docstring = True

nbsphinx_prolog = """
Download this Jupyter notebook from `github <https://github.com/strawpants/geoslurp/blob/main/docs/source/notebooks/{{ env.doc2path(env.docname, base=None) }}>`_

----
"""


html_favicon = '_static/favicon.ico'
