class StopUpdateHeartbeat(BaseException):
    def __str__(self):
        return "Stop updating heartbeat now."
