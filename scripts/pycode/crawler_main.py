import os

from crawler_components import (
    ExampleDataHandler,
    RequestsExampleHTTPResponseParser,
    RequestsHTTPRequest,
)

from smoothcrawler_cluster import ZookeeperCrawler
from smoothcrawler_cluster.model.metadata import RunningContent

_RUNNER_CRAWLER_VALUE = int(os.getenv("CLUSTER_RUNNER", 2))
_BACKUP_CRAWLER_VALUE = int(os.getenv("CLUSTER_BACKUP", 1))
_CRAWLER_NAME = os.getenv("CRAWLER_NAME", "sc-crawler_1")
_ZK_HOSTS = os.getenv("ZK_HOSTS", "localhost:2181")
_RUN_ONE_TASK = os.getenv("RUN_ONE_TASK", "true")


initial = False if _RUN_ONE_TASK == "true" else True

zk_crawler = ZookeeperCrawler(
    runner=_RUNNER_CRAWLER_VALUE,
    backup=_BACKUP_CRAWLER_VALUE,
    name=_CRAWLER_NAME,
    initial=initial,
    ensure_initial=True,
    ensure_timeout=100,
    ensure_wait=1,
    zk_hosts=_ZK_HOSTS,
)
zk_crawler.register_factory(
    http_req_sender=RequestsHTTPRequest(),
    http_resp_parser=RequestsExampleHTTPResponseParser(),
    data_process=ExampleDataHandler(),
)

if _RUN_ONE_TASK == "true":
    task_content = RunningContent(
        task_id=0, url="https://www.example.com", method="GET", header={}, parameters={}, body={}
    )
    result = zk_crawler.processing_crawling_task(content=task_content)
    print(f"[DEBUG] result: {result}")
else:
    zk_crawler.run()
