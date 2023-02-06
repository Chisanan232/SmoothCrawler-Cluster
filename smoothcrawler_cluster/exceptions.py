"""*Global exception module*

Some exceptions or errors for this package *SmoothCrawler-Cluster*'s usage.
"""


class CrawlerIsDeadError(RuntimeError):
    """This error for module *dispatcher*. Dispatcher object would raise this error if dead role calls it."""

    def __init__(self, crawler_name: str, group: str):
        """

        Args:
            crawler_name (str): The current crawler instance's name.
            group (str): The group which current crawler instance is in.
        """
        self._crawler_name = crawler_name
        self._group = group

    def __str__(self):
        return f"Current crawler instance '{self._crawler_name}' in group '{self._group}' is dead."


class StopUpdateHeartbeat(BaseException):
    """This error for module *workflow*. The signal of stopping updating heartbeat."""

    def __str__(self):
        return "Stop updating heartbeat now."
