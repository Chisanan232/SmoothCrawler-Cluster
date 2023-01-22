==============================================
Depend on third party application - Zookeeper
==============================================

Managing the meta-data objects by `Zookeeper <https://zookeeper.apache.org/documentation.html>`_. So any meta-data operations
would be through by Zookeeper.

Crawler which supports to manage meta-data by Zookeeper:

* **ZookeeperCrawler**

In *SmoothCrawler-Cluster*, it could use **ZookeeperCrawler** to setup a crawler cluster which let Zookeeper manages meta-data.
We could import it as following:

.. code-block:: python

    from smoothcrawler_cluster import ZookeeperCrawler

And remember it must set the Zookeeper host(s) by argument ``zk_hosts``.

.. code-block:: python

    zk_crawler = ZookeeperCrawler(runner=5,
                                  name="crawler_<index>",
                                  zk_hosts="localhost:2181")    # Don't forget to set this option!

It could use ``zkCli.sh`` to check the data in Zookeeper after we setup the crawler cluster.

.. code-block:: shell

    root@d8e0a4b0c3f4:/apache-zookeeper-3.8.0-bin# ./bin/zkCli.sh
    Connecting to localhost:2181
    ...
    Welcome to ZooKeeper!
    ...
    WatchedEvent state:SyncConnected type:None path:null
    [zk: localhost:2181(CONNECTED) 0]

In generally, it could use general CRUD commands to verify data operations like ``get``:

.. code-block:: shell

    [zk: localhost:2181(CONNECTED) 2] get /smoothcrawler/group/sc-crawler-cluster/state
    {"total_crawler": 3, "total_runner": 2, "total_backup": 1, "standby_id": "3", "current_crawler": ["sc-crawler_1", "sc-crawler_2", "sc-crawler_3"], "current_runner": ["sc-crawler_1", "sc-crawler_2", "sc-crawler_1", "sc-crawler_2", "sc-crawler_1", "sc-crawler_2"], "current_backup": ["sc-crawler_3", "sc-crawler_3", "sc-crawler_3"], "fail_crawler": [], "fail_runner": [], "fail_backup": []}
