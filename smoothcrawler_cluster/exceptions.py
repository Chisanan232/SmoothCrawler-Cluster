"""*Global exception module*

Some exceptions or errors for this package *SmoothCrawler-Cluster*'s usage.
"""


class StopUpdateHeartbeat(BaseException):
    def __str__(self):
        return "Stop updating heartbeat now."
