.. _Installation:

=============
Installation
=============

*SmoothCrawler-Cluster* is installable using a Python package manager. *SmoothCrawler-Cluster* is depends on another one package
*SmoothCrawler*, so it would also installs it if you never install it before.

For installing package, it has 2 ways to install it recommends with different motivations: using ``pip`` or ``git clone``.

Install by pip
===============

Using ``pip`` to install:

.. code-block:: shell

    >>> pip install smoothcrawler-cluster

You should see the installed packages: *SmoothCrawler* and *SmoothCrawler-Cluster*.

.. code-block:: shell

    >>> pip list
    Package                       Version
    ----------------------------- -----------
    ...
    SmoothCrawler                 0.2.0
    SmoothCrawler-Cluster         0.1.0

Install by setup.py from GitHub
================================

If you want to experience the latest features, you could install it through ``git clone`` from GitHub and run command line:

* Clone the project from GitHub by ``git``:

.. code-block:: shell

    >>> git clone https://github.com/Chisanan232/SmoothCrawler-Cluster ./apache-smoothcrawler-cluster

.. note::

    You also could select one specific git branch to clone and install to get a taste of different state of package in different branch.

    * *develop/**: In progressing of development. In generally, the latest features must be in here.
    * *release*: Pre-release
    * *master*: Official release (tag version in GitHub or push to PyPI)

* Install the package by ``setup.py``:

.. code-block:: shell

    >>> python3 ./apache-smoothcrawler-cluster/setup.py install
