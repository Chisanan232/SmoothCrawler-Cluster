========
Crawler
========

Crawler Module
===============

* Module: *smoothcrawler_cluster.crawler*
* API reference: :ref:`CrawlerAPIRef`

.. _Crawler_module_UML:

UML
----

.. image:: ../../../images/development_document/software_architecture/crawler_uml.drawio.png

Description
------------

It has 3 layers of the crawler's extending: **BaseDistributedCrawler**, **BaseDecentralizedCrawler** and *SmoothCrawler*'s
**BaseCrawler**, **ZookeeperCrawler**. We could divide them as below:

* **BaseDistributedCrawler**: Primary base class layer
* **BaseDecentralizedCrawler** and *SmoothCrawler*'s **BaseCrawler**: Second base class layer
* **ZookeeperCrawler**: Implementation layer

*SmoothCrawler-Cluster* is a package which encapsulates some features to let *SmoothCrawler* could have more higher fault
tolerance as a cluster system. But for a cluster system, it could be briefly divided to 2 different types: **Centralize**
and **Decentralize**. So it also follows this concept to design the software architecture of *SmoothCrawler-Cluster*.

Primary base class layer
~~~~~~~~~~~~~~~~~~~~~~~~~

Modules:

* **BaseDistributedCrawler**

This is the base class of all second base classes. However, it just rules the most foundational features about cluster. It
won't have some very clear design or direction to let sub-class to know it should be centralize, decentralize or something
else object.

.. attention::

    Because the primary base class doesn't rule any clear functions to let sub-class could be have more apparently directions,
    it should NOT be extended by *Implementation layer* directly. It ONLY for **Secondary base layer**.

Secondary base class layer
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Modules:

* **BaseCentralizedCrawler**  (would be support in version *0.2.0*)
* **BaseDecentralizedCrawler**
* *SmoothCrawler*'s **BaseCrawler**

The second base classes which extends the primary base one to let its design and direction to be more clear. So the objects
in this layer would be apparently focus on one direction like **centralize cluster** or **decentralize cluster**.

Here layer also includes *SmoothCrawler*'s **BaseCrawler** because all of *SmoothCrawler* should be ruled by it.

.. note::

    This layer is helpful for **Implementation layer** to implement. So all the objects in **Implementation layer** should
    extend **Secondary base class**.

Implementation layer
~~~~~~~~~~~~~~~~~~~~~

Modules:

* **ZookeeperCrawler**

The layer is the crawler implementation. But it won't extend all secondary base classes, it would only extend one of it, i.e.,
**ZookeeperCrawler** only extends **BaseDecentralizeCrawler** and implements its features.

Crawler Module's relation with other modules
=============================================

This is the core of *SmoothCrawler-Cluster* package, so it would be more complicate. Here demonstrate the relation between

.. _Crawler_module_relation_UML:

UML
----

.. image:: ../../../images/development_document/software_architecture/crawler_relation_uml.drawio.png

Description
------------

*Crawler* module has aggregation relation with many classes in different modules. Here would clear that which modules is in
the relation and why use it in *crawler* module.

Aggregation with *zookeeper* module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Crawler would use *zookeeper* module to build session and do some operations with Zookeeper.

Aggregation with *converter* module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All the meta-data objects would be processed by *converter* module.

Aggregation with *election* module
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

About running **Runner** election, it would use the strategy in *election* module to run.
