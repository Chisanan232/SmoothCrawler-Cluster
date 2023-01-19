==========
No Leader
==========

In infra design of *SmoothCrawler-Cluster* usage, it provides some different way to developers to consider and implement it.

The direction of this section is **cluster without leader**. All members would be the same level in a no leader cluster, i.e.,
they don't have *leader* or *master*, they all are *follower* or *slave*. In the other words, it is **decentralized**. If they
all are *follower* or *slave*, it has 2 types in running: all of them are **Runner** or some of them are **Runner**. Hence,
below are 2 usage guide lines for developers to refer and consider.

.. toctree::
   :maxdepth: 2
   :titlesonly:

   all_are_runner
   some_are_runner
