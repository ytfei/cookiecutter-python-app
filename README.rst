###################################
Python Application Project Template
###################################

|badge|

This is a `Cookiecutter`_ template for creating a Python application project.

The project layout is based on the `Python Packaging User Guide`_. The current
conventional wisdom forgoes the use of a source directory, but moving the
package out of the project root provides several advantages (*cf.*
`Packaging a Python library`_).


The `py27`_ branch is for Python 2.7 compatibility; it is no longer actively
maintained.


================
Project Features
================

- Python 3.6+
- `MIT License`_
- `pytest`_ test suite
- `Sphinx`_ documentation


====================
Application Features
====================

- CLI with subcommands
- Logging
- Hierarchical `YAML`_ configuration


=====
Usage
=====

Install Python requirements for using the template:

.. code-block:: console

  $ python -m pip install --user --requirement=requirements.txt
  $ PYTHONPATH=./src python -m jd_edu_sport_fix.cli hello


Create a new project directly from the template on `GitHub`_:

.. code-block:: console

  $ cookiecutter gh:mdklatt/cookiecutter-python-app


.. _travis: https://travis-ci.org/mdklatt/cookiecutter-python-app
.. |badge| image:: https://travis-ci.org/mdklatt/cookiecutter-python-app.png
    :alt: Travis CI build status
    :target: `travis`_
.. _Cookiecutter: http://cookiecutter.readthedocs.org
.. _Python Packaging User Guide: https://packaging.python.org/en/latest/distributing.html#configuring-your-project
.. _Packaging a Python library: http://blog.ionelmc.ro/2014/05/25/python-packaging
.. _py27: https://github.com/mdklatt/cookiecutter-python-app/tree/py27
.. _pytest: http://pytest.org
.. _Sphinx: http://sphinx-doc.org
.. _MIT License: http://choosealicense.com/licenses/mit
.. _YAML: http://pyyaml.org/wiki/PyYAML
.. _GitHub: https://github.com/mdklatt/cookiecutter-python-app
