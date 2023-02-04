"""
Global exceptions module

Some customized exceptions for this package SmoothCrawler-Cluster.
"""


class ZookeeperCrawlerNotReady(RuntimeError):
    def __str__(self):
        return "Current crawler instance is not ready for running. Its *current_runner* still be empty."


class StopUpdateHeartbeat(BaseException):
    def __str__(self):
        return "Stop updating heartbeat now."
