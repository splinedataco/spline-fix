import logging
import threading
from typing import Callable, Union

from fix.reader import SessionContext

"""NOTE:
There is also a HeartbeatTimer in
utils.downloader.datacapture.msrb.rtrs.HeartBeatTimer
But it's resolution is in seconds. Rather than chaning that code
we are just making another here that does something similar.
"""


class HeartbeatTimer:
    """HeartbeatTimer calls a function when the timeout_ms has expired.
    So in the lambda that is passed in to the callable, it will need to
    have some type of shared object that it can check to determine what
    course of action to take.
    For example: for the server heartbeat, there should be a function/lambda
    that captures the context and knows to look at both the server time
    of the last message, and if it needs to reschedule itself how far
    in the future to do so. If the distance between the last message
    and now is a multiple of the timeout, with three as common choice,
    then the function will need to be able to terminate the session.
    """

    _session_context: SessionContext

    def __init__(
        self, fn: Callable, context: SessionContext, timeout_ms: Union[int, float] = 0.1
    ) -> None:
        in_ms: float
        if isinstance(timeout_ms, float):
            in_ms = timeout_ms
        elif isinstance(timeout_ms, int):
            in_ms = timeout_ms / 1000.0
        self.heartbeatTimer = threading.Timer(interval=in_ms, function=fn)

    """ stops the timer thread from running. Effectively stops it.
    """

    def cancel(self) -> None:
        logging.debug("heartbeat.cancel() start")
        self.heartbeatTimer.cancel()
        logging.debug("heartbeat.cancel() returning")

    def reset(self) -> None:
        logging.debug("heartbeat.reset() start")
        self.heartbeatTimer.cancel()
        logging.debug("heartbeat.reset() creating new threaded Timer")
        self.heartbeatTimer = threading.Timer(
            interval=self.heartbeatTimer.interval, function=self.heartbeatTimer.function
        )

        logging.debug("heartbeat.reset() calling timer.start()")
        self.start()

    def start(self) -> None:
        self.heartbeatTimer.daemon = True
        self.heartbeatTimer.start()

    def purge(self) -> None:
        self.reset()
