====================
Zookeeper Crawlers
====================

*module* smoothcrawler.crawler

Here are the module which has many different **Crawler roles** for different scenarios.
They also are the 'final production' which combines the needed components as web spider
and uses that features.

So **components** implement what it works at each processes, **crawler role** implement
how it works with its components.


.. autoclass:: smoothcrawler_cluster.crawler.ZookeeperCrawler
    :private-members: _is_ready_by_groupstate, _update_crawler_role, _run_updating_heartbeat_thread, _update_heartbeat, _run_crawling_processing, _chk_register
    :members:
