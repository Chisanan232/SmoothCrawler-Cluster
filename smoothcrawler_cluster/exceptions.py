"""*Global exception module*

Some exceptions or errors for this package *SmoothCrawler-Cluster*'s usage.
"""


class CrawlerIsDeadError(RuntimeError):
    def __init__(self, crawler_name: str, group: str):
        self._crawler_name = crawler_name
        self._group = group

    def __str__(self):
        return f"Current crawler instance '{self._crawler_name}' in group '{self._group}' is dead."


class StopUpdateHeartbeat(BaseException):
    def __str__(self):
        return "Stop updating heartbeat now."
