.. SmoothCrawler-Cluster documentation master file, created by
   sphinx-quickstart on Thu Jul 21 09:49:56 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

SmoothCrawler-Cluster
=======================

|python-versions| |release-version| |pypi-version| |license| |github-actions build-status| |codecov-coverage| |pylint| |codacy-level|

*A Python package which is encapsulation of high fault tolerance features for building cluster or distributed crawler system with *SmoothCrawler*.*


Overview
----------

Do you ever been troubled how to develop a high fault tolerance program of crawler? In generally, crawler must gets fails or other
weired issues so that occur any exceptions in running for a while because the Font-End code always be changed (no matter for new
feature, breaking change or against web spider 🫣 ). So about the *SmoothCrawler*, it targets to separating the different concerns
to different objects to let every components could focus on what thing they should and only to do; about the *SmoothCrawler-Cluster*,
it extends the SoC features as a distributed or cluster system further, it gives crawler has high fault tolerance feature!

However, we could develop a high fault tolerance crawler easily. Let's demonstrate an example to show how easy and clear it is!

.. code-block:: python

   from smoothcrawler_cluster import ZookeeperCrawler

   # Instantiate some crawlers with Zookeeper
   zk_crawler = ZookeeperCrawler(runner=2,
                                 backup=1,
                                 ensure_initial=True,
                                 zk_hosts=_ZK_HOSTS)
   # Same as SmoothCrawler general usage, register the components into factory
   zk_crawler.register_factory(http_req_sender=RequestsHTTPRequest(),
                               http_resp_parser=RequestsExampleHTTPResponseParser(),
                               data_process=ExampleDataHandler())
   # Listen and wait for tasks
   zk_crawler.run()


General Documentation
----------------------

This part of documentation, which introduces the package and has some step-by-step instructions for using or building a crawler with
high fault tolerance features.

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   introduction
   installation
   quickly_start
   advanced_usage/index


Usage Guides
-------------

This section for who racks his/her brain developing and designing his/her own customized crawler but still have no idea.

.. toctree::
   :maxdepth: 1
   :caption: Usage Guides:

   usage_guides/no_leader/index
   usage_guides/has_leader/index
   usage_guides/handle_metadata/index


API Reference
---------------

Detail information about some function, class or method.

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :caption: API Reference:

   api_references/crawler/index
   api_references/election/index
   api_references/model/index
   api_references/inner_modules/index


Development Documentation
--------------------------

If you're curious about the detail of implementation of this package includes workflow, software architecture, system design or
development, this section is for you.

.. toctree::
   :caption: Development Documentation
   :maxdepth: 1

   development_document/flow
   development_document/software_architecture
   development_document/test
   development_document/ci-cd


Change Logs
------------

Release information.

.. toctree::
   :caption: Change Logs
   :maxdepth: 1

   release_notes/0.X/index



.. |python-versions| image:: https://img.shields.io/pypi/pyversions/SmoothCrawler-Cluster.svg?logo=python&logoColor=FBE072
    :alt: Python version support
    :target: https://pypi.org/project/SmoothCrawler-Cluster


.. |release-version| image:: https://img.shields.io/github/v/release/Chisanan232/SmoothCrawler-Cluster.svg?logo=github&color=orange
    :alt: Package release version in GitHub
    :target: https://github.com/Chisanan232/SmoothCrawler-Cluster/releases


.. |pypi-version| image:: https://img.shields.io/pypi/v/SmoothCrawler-Cluster?color=%23099cec&label=PyPI&logo=pypi&logoColor=white
    :alt: Package version in PyPi
    :target: https://pypi.org/project/SmoothCrawler-Cluster/


.. |license| image:: https://img.shields.io/badge/License-Apache%202.0-blue.svg?logo=apache
    :alt: License
    :target: https://opensource.org/licenses/Apache-2.0


.. |circle-ci build-status| image:: https://circleci.com/gh/Chisanan232/multirunnable.svg?style=svg
    :alt: Circle-CI building status
    :target: https://app.circleci.com/pipelines/github/Chisanan232/multirunnable


.. |github-actions build-status| image:: https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml/badge.svg?branch=master
    :alt: GitHub-Actions building status
    :target: https://github.com/Chisanan232/SmoothCrawler-Cluster/actions/workflows/ci-cd.yml


.. |codecov-coverage| image:: https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster/branch/master/graph/badge.svg?token=H34TPZQXYL
    :alt: Test coverage with 'codecov'
    :target: https://codecov.io/gh/Chisanan232/SmoothCrawler-Cluster


.. |coveralls-coverage| image:: https://coveralls.io/repos/github/Chisanan232/SmoothCrawler-Cluster/badge.svg?branch=develop/ci-cd
    :alt: Test coverage with 'coveralls'
    :target: https://coveralls.io/github/Chisanan232/SmoothCrawler-Cluster?branch=develop/ci-cd


.. |pylint| image:: https://img.shields.io/badge/linting-pylint-black
    :alt: Code Quality checking tool with Pylint
    :target: https://github.com/PyCQA/pylint


.. |codacy-level| image:: https://app.codacy.com/project/badge/Grade/171272bee2594687964f1f4473628a0f
    :alt: Code Quality by Codacy
    :target: https://www.codacy.com/gh/Chisanan232/SmoothCrawler-Cluster/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Chisanan232/SmoothCrawler-Cluster&amp;utm_campaign=Badge_Grade


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
