import argparse

from crawler_components import (
    ExampleDataHandler,
    RequestsExampleHTTPResponseParser,
    RequestsHTTPRequest,
)

from smoothcrawler_cluster import ZookeeperCrawler

parser = argparse.ArgumentParser(description="A sample crawler in cluster")
parser.add_argument("--runner", type=int, default=None)
parser.add_argument("--backup", type=int, default=None)
parser.add_argument("--crawler-name", type=str, default="sc-crawler_1")
args = parser.parse_args()

_RUNNER_CRAWLER_VALUE = args.runner
_BACKUP_CRAWLER_VALUE = args.backup
_CRAWLER_NAME = args.crawler_name


zk_crawler = ZookeeperCrawler(
    runner=_RUNNER_CRAWLER_VALUE,
    backup=_BACKUP_CRAWLER_VALUE,
    name=_CRAWLER_NAME,
    ensure_initial=True,
    ensure_timeout=10,
    ensure_wait=1,
    zk_hosts="localhost:2181",
)
zk_crawler.register_factory(
    http_req_sender=RequestsHTTPRequest(),
    http_resp_parser=RequestsExampleHTTPResponseParser(),
    data_process=ExampleDataHandler(),
)
zk_crawler.run()
