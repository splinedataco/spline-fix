from enum import Enum
from io import BytesIO, StringIO
from typing import Any, Callable, cast, Dict, Optional, Union, Tuple
import logging
from multiprocessing.connection import Connection
import multiprocessing.sharedctypes
from multiprocessing.sharedctypes import Synchronized
from threading import Timer
import multiprocessing as mp
import ctypes
import datetime as dt
from datetime import timedelta

from joshua.fix.sofh import Sofh
from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.fixp.messages import Jwt, Uuid
from joshua.fix.fixp.sequence import Sequence, UnsequencedHeartbeat
from joshua.fix.types import SplineNanoTime, SplineDateTime
from joshua.fix.generate import generate_message_from_type
from joshua.fix.fixp.terminate import TerminationCodeEnum

# mp_ctx = mp.get_context("spawn")
# mp_ctx = mp.get_context("fork")
start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
# logger = mp_ctx.get_logger()
logger = logging.getLogger()
# logger.setLevel(logging.root.level)
# logger.setLevel(logging.WARNING)
"""client: initiate Negotiation -> server: receive Negotiate
   server: NegotiateReject -> SendTerminateTransport -> WaitingForClientTerminate -> TerminateTransport -> close Socket

class ReaderStateEnum(Enum):
    WaitingForNegotiate = (0,)
    SendNegotiationReject,
    SendTerminateTransport,
    NegotiatingResponse,
    NegotiationResponseAccepted,
"""


class SessionStateEnum(Enum):
    WaitingOnConnect = 0
    Connected = 1
    Terminate = 2
    Terminating = 3
    Terminated = 4
    Negotiating = 5
    Negotiated = 6
    Establishing = 7
    Established = 8


class SessionContext:
    # Lock is a method/constructor, the syncronize.Lock is the type
    # it returns
    # store as a timestamp so is a fundamental ctype
    # that may be stored in shared memory easily
    _recv_time_last_message: float  # Synchronized[ctypes.c_double]
    _send_time_last_message: float  # Synchronized[ctypes.c_double]

    # shouldn't need a lock as they should not change
    # after negotiate
    _recv_hb_interval_ms: int  # Synchronized[ctypes.c_uint64]
    _send_hb_interval_ms: int  # Synchronized[ctypes.c_uint64]

    _recv_hb_timer: Timer
    _send_hb_timer: Timer

    # current sequence number, not the next sequence number
    # why optional? Because on a None type feed it MUST
    # not send sequenced messages, and therefore doesn't
    # have a next sequence number.
    _send_sequence: int  # Synchronized[ctypes.c_uint64]
    _recv_sequence: int  # Synchronized[ctypes.c_uint64]

    _curves_permitted: bool  # Synchronized[ctypes.c_bool]
    _predictions_permitted: bool  # Synchronized[ctypes.c_bool]

    _is_client: bool = True

    _heartbeat_recv_expired_fn: Callable
    _heartbeat_send_expired_fn: Callable

    _session_state: int  # Synchronized[ctypes.c_uint8]

    _credentials: Jwt

    @property
    def is_client(self) -> bool:
        return self._is_client

    @is_client.setter
    def is_client(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise NotImplementedError
        self._is_client = value

    @property
    def recv_hb_interval_ms(self) -> int:
        return self._recv_hb_interval_ms.value

    @recv_hb_interval_ms.setter
    def recv_hb_interval_ms(self, value: int) -> None:
        logging.debug(f"client={self.is_client} recv_hb_interval_ms = {value}")
        if isinstance(value, int | float):
            v = int(value)
            # NOTE: nagles is 40ms
            # if given an interval under 10ms
            # or over 30 seconds
            if v < 10 or v > 30_000:
                raise ValueError(f"interval must be between 10 and 30000 ms. Got: {v}")
            self._recv_hb_interval_ms.value = v
        else:
            raise NotImplementedError

        try:
            self._recv_hb_timer
            # then it exists and we want to cancel it
            logging.debug(
                f"client={self.is_client} resetting interval timer in recv_hb_interval_ms"
            )
            self.reset_recv_timer()
        except AttributeError:
            # doesn't exist yet, so set it
            logging.debug(
                f"client={self.is_client} recv_hb_timer doesn't exist. Resetting interval timer in recv_hb_interval_ms"
            )

            # need to have one before we can reset it.
            self._recv_hb_timer = Timer(
                self.recv_hb_interval_ms / 1000,
                self.heartbeat_expired_recv,
            )

    @staticmethod
    def cancel_timer(timer: Timer) -> None:
        if timer.is_alive():
            timer.cancel()

    @property
    def send_hb_interval_ms(self) -> int:
        return self._send_hb_interval_ms.value

    @send_hb_interval_ms.setter
    def send_hb_interval_ms(self, value: int) -> None:
        logging.debug(f"client={self.is_client} send_hb_interval_ms = {value}")
        if isinstance(value, int | float):
            v = int(value)
            # NOTE: nagles is 40ms
            # if given an interval under 10ms
            # or over 30 seconds
            if v < 10 or v > 30_000:
                raise ValueError(f"interval must be between 10 and 30000 ms. Got: {v}")
            self._send_hb_interval_ms.value = v
        else:
            logging.error(
                "send_hb_interval_ms set failed type:value {}:{}".format(
                    type(value), value
                )
            )
            raise NotImplementedError
        try:
            self._send_hb_timer
            # then it exists and we want to cancel it
            logging.debug(
                f"is_client={self.is_client} send_hb_interval_ms resetting timer"
            )
            self.cancel_timer(self._send_hb_timer)
            # and reset it
            self._send_hb_timer = Timer(
                self.send_hb_interval_ms / 1000, self.heartbeat_expired_send
            )
        except AttributeError:
            # doesn't exist yet, so set it
            logging.debug(
                f"is_client={self.is_client} doesn't have send_hb_timer. Resetting timer"
            )
            self._send_hb_timer = Timer(
                self.send_hb_interval_ms / 1000, self.heartbeat_expired_send
            )

    @property
    def send_time_last_message_timestamp(self) -> float:
        return cast(float, self._send_time_last_message.value)

    @send_time_last_message_timestamp.setter
    def send_time_last_message_timestamp(self, value: Any) -> None:
        # lie about type to make mypy shut up
        self.set_send_time_last_message(value)

    @property
    def send_time_last_message(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.send_time_last_message_timestamp)

    @send_time_last_message.setter
    def send_time_last_message(self, value: Any) -> None:
        return self.set_send_time_last_message(value)

    def set_send_time_last_message(self, value: Any) -> None:
        # logging.debug(
        #     f"client={self.is_client} send_time_last_message(type={type(value)} {value})"
        # )
        tmp_timestamp: float
        if value is None:
            tmp_timestamp = dt.datetime.now().timestamp()
            logging.debug(
                f"value was None tmp_timestamp set to {tmp_timestamp} or {dt.datetime.fromtimestamp(tmp_timestamp)}"
            )
        elif isinstance(value, dt.datetime):
            tmp_timestamp = value.timestamp()
        elif isinstance(value, SplineNanoTime):
            tmp_timestamp = value.timestamp
        elif isinstance(value, SplineDateTime):
            tmp_timestamp = value.timestamp
        else:
            raise NotImplementedError
        # logging.debug(
        #     "client={} send_time_last_message setting to: {}".format(
        #         self.is_client, tmp_timestamp
        #     )
        # )
        # logging.debug(
        #     f"client={self.is_client}.send_time_last_message was = {self.send_time_last_message} {self.send_time_last_message_timestamp}"
        # )

        with self._send_time_last_message.get_lock():
            self._send_time_last_message.value = cast(ctypes.c_double, tmp_timestamp)
        # logging.debug(
        #     f"client={self.is_client}.send_time_last_message now = {self.send_time_last_message} {self.send_time_last_message_timestamp}"
        # )

    @property
    def recv_time_last_message_timestamp(self) -> float:
        return cast(float, self._recv_time_last_message.value)

    @recv_time_last_message_timestamp.setter
    def recv_time_last_message_timestamp(self, value: Any) -> None:
        # just lie about the type, we're checking it in the setter.
        self.set_recv_time_last_message(value)

    @property
    def recv_time_last_message(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.recv_time_last_message_timestamp)

    def set_recv_time_last_message(self, value: Any) -> None:
        logging.debug(
            f"client={self.is_client} recv_time_last_message(type={type(value)} {value})"
        )
        tmp_timestamp: float
        if value is None:
            logging.debug(
                f"client({self.is_client}) value is None set to now().timestamp()"
            )
            tmp_timestamp = dt.datetime.now().timestamp()
            logging.debug(
                f"client({self.is_client}) value is None set to {tmp_timestamp} {dt.datetime.fromtimestamp(tmp_timestamp)}"
            )
        elif isinstance(value, dt.datetime):
            tmp_timestamp = value.timestamp()
        elif isinstance(value, SplineNanoTime):
            tmp_timestamp = value.timestamp
        elif isinstance(value, SplineDateTime):
            tmp_timestamp = value.timestamp
        else:
            raise NotImplementedError
        logging.debug(
            f"client={self.is_client}.recv_time_last_message was = {self.recv_time_last_message} {self.recv_time_last_message_timestamp}"
        )

        with self._recv_time_last_message.get_lock():
            # logging.debug(
            #     f"client({self.is_client}) in lock: _recv_time_last_message.value = {self._recv_time_last_message.value}"
            # )
            self._recv_time_last_message.value = cast(ctypes.c_double, tmp_timestamp)
            # logging.debug(
            #     f"client({self.is_client}) in lock: _recv_time_last_message.value now = {self._recv_time_last_message.value}"
            # )
        # logging.debug(
        #     f"outside lock client={self.is_client}.recv_time_last_message now = {self.recv_time_last_message}\
        #     {self.recv_time_last_message_timestamp} _recv_time_last_message.value={self._recv_time_last_message.value} tmp_timestamp={tmp_timestamp}"
        # )

    def next_send_heartbeat_milliseconds(self) -> int:
        """Here in case we wanted to do something more sophisticated
        with the timer. In the non-sophisticated manner we can just
        return the heartbeat interval in milliseconds
        """
        return self.recv_hb_interval_ms

    def next_recv_heartbeat_milliseconds(self) -> int:
        """Here in case we wanted to do something more sophisticated
        with the timer. In the non-sophisticated manner we can just
        return the heartbeat interval in milliseconds
        """
        return self.send_hb_interval_ms

    def next_send_heartbeat(self) -> dt.datetime:
        return self.send_time_last_message + timedelta(
            seconds=+self.send_hb_interval_ms
        )

    def next_recv_heartbeat(self) -> dt.datetime:
        return self.recv_time_last_message + timedelta(
            seconds=+self.recv_hb_interval_ms
        )

    def noop(self) -> None:
        pass

    @property
    def credentials(self) -> Jwt:
        return self._credentials

    @credentials.setter
    def credentials(self, cred: Union[Jwt, bytes]) -> None:
        if isinstance(cred, bytes):
            cred = Jwt(token=cred)
        if isinstance(cred, Jwt):
            logging.debug(
                "Negotiate.__init__ got Jwt. Assigning to credentials. {}".format(cred)
            )
            self._credentials = cred
        else:
            raise NotImplementedError
        # when here, we've set something
        # now we want to make sure that we set the permissions appropriately
        self._curves_permitted.value = self._credentials.curves_permitted
        self._predictions_permitted.value = self._credentials.predictions_permitted

    """To handle authentication and ability to use data, we need to do something different
    with checks for curves_permitted and predictions_permitted.
    We're using multiple processes to do simultaneous processing. One way we do this is to
    have a separate process monitor changes and then send the data to all processes. Each
    process then needs to be able to indicate if they are allowed to have the data published
    for them; the actual publishing of the data is over a shared file descriptor and doesn't
    need any further additional configuration.

    On the server side in particular, because it receives the Jwt and authentication information
    after the instance is spawned, when the parent or sibling later tries to publish to the shared fd
    the Jwt state hasn't been updated, so the parent/sibling continues using the default, which is to
    deny access.

    The "easy" way we're going to do this is to "pull up" all of the shared values into simple types
    in the context that will be shared between processes. When the Jwt is set, the values
    will be "lifted" out of the jwt and stored in the shared object itself. Then, checks for permissions
    will be done from the context rather than from reaching into the context to get the jwt.
    """

    def curves_permitted(self) -> bool:
        return self._curves_permitted.value

    def predictions_permitted(self) -> bool:
        return self._predictions_permitted.value

    def __init__(
        self,
        conn: Connection,
        is_client: Optional[bool] = True,
        credentials: Optional[Jwt] = None,
    ) -> None:
        self._curves_permitted = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_bool, False, lock=True)
        )
        self._predictions_permitted = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_bool, False, lock=True)
        )

        if credentials is not None:
            self.credentials = credentials
        else:
            self.credentials = Jwt()
        # mp_ctx = mp.get_context("spawn")
        # set so there is some function before a function is assigned
        self._heartbeat_recv_expired_fn = self.noop
        self._heartbeat_send_expired_fn = self.noop
        self._connection = conn
        self.connection_lock = mp_ctx.RLock()
        # using Synchronized() as suggested as a workaround doesn't work. The actual type returned is SynchronizedBase
        # and Synchronized() is a more specific sub-type. But without this, mypy complains.
        # https://github.com/python/typeshed/issues/8799
        # self._server_sequence = Synchronized(Value(c_uint64, 0, lock=self._server_lock))
        # TODO: maybe keep send_sequence as None for is_client==True
        self._send_sequence = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._recv_sequence = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._session_state = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint8, 0, lock=True)
        )
        self._recv_time_last_message = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._send_time_last_message = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._recv_hb_interval_ms = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._send_hb_interval_ms = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True)
        )
        self._session_id = Uuid()
        self._other_end_connection: Optional[Connection] = None
        # only make the call 1x
        tm_now = dt.datetime.now().timestamp()
        logger.debug(f"reader.__init__ set _recv_time_last_message = Value({tm_now})")
        self._recv_time_last_message = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_double, tm_now, lock=True)
        )
        logging.debug(
            f"reader.__init__ _recv_time_last_message now = Value({self._recv_time_last_message.value})"
        )
        self._send_time_last_message = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_double, tm_now, lock=True)
        )
        self.recv_hb_interval_ms = 1_000
        self.send_hb_interval_ms = 1_000
        logging.debug(f"SessionContext.__init__() setting is_client to {is_client}")
        self.is_client = cast(bool, is_client)

    @property
    def send_sequence(self) -> int:
        return cast(int, self._send_sequence.value)

    @send_sequence.setter
    def send_sequence(self, value: Union[int, None]) -> None:
        if value is None:
            self._send_sequence = cast(
                Synchronized,
                mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True),
            )
        elif isinstance(value, int):
            if value < 0:
                raise ValueError(f"sequence numbers must be non-negative. Got: {value}")
            if self._send_sequence is None:
                self._send_sequence = cast(
                    Synchronized,
                    mp.sharedctypes.Value(ctypes.c_uint64, value, lock=True),
                )
            else:
                self._send_sequence.value = cast(ctypes.c_uint64, value)
        else:
            raise NotImplementedError

    @property
    def recv_sequence(self) -> int:
        return cast(int, self._recv_sequence.value)

    @recv_sequence.setter
    def recv_sequence(self, value: Optional[int]) -> None:
        if value is None:
            self._recv_sequence = cast(
                Synchronized,
                mp.sharedctypes.Value(ctypes.c_uint64, 0, lock=True),
            )

        elif isinstance(value, int):
            if value < 0:
                raise ValueError(f"sequence numbers must be non-negative. Got: {value}")
            if self._recv_sequence is None:
                self._recv_sequence = cast(
                    Synchronized,
                    mp.sharedctypes.Value(ctypes.c_uint64, value, lock=True),
                )
            else:
                self._recv_sequence.value = cast(ctypes.c_uint64, value)
        else:
            raise NotImplementedError

    @property
    def session_id(self) -> Optional[Uuid]:
        return self._session_id

    @session_id.setter
    def session_id(self, value: Uuid) -> None:
        if not isinstance(value, Uuid):
            raise NotImplementedError
        self._session_id = value

    @property
    def connection(self) -> Connection:
        return self._connection

    @connection.setter
    def connection(self, conn: Connection) -> None:
        if not isinstance(conn, Connection):
            raise NotImplementedError
        self._connection = conn

    @property
    def other_end_connection(self) -> Optional[Connection]:
        return self._other_end_connection

    @other_end_connection.setter
    def other_end_connection(self, conn: Connection) -> None:
        if not isinstance(conn, Connection):
            raise NotImplementedError
        self._other_end_connection = conn

    @property
    def session_state(self) -> SessionStateEnum:
        """Just an alias for server_state for now"""
        return SessionStateEnum(self._session_state.value)

    @session_state.setter
    def session_state(self, value: Union[int, SessionStateEnum]) -> None:
        """Just an alias for server_state for now"""
        if isinstance(value, int):
            self.server_state = SessionStateEnum(value)
        else:
            self.server_state = value

    @property
    def server_state(self) -> SessionStateEnum:
        """This is server_state and not session_state
        because state can be held by both a client or a server,
        as each will be in different states that permit receiving
        certain messages.
        """
        return SessionStateEnum(self._session_state.value)

    @server_state.setter
    def server_state(self, value: Union[int, SessionStateEnum]) -> None:
        target_val: int = 0
        if isinstance(value, int):
            target_val = int(value)
        elif isinstance(value, SessionStateEnum):
            target_val = int(value.value)

        with self._session_state.get_lock():
            self._session_state.value = cast(ctypes.c_ubyte, target_val)

    @property
    def send_seq(self) -> int:
        return cast(int, self._send_sequence.value)

    @send_seq.setter
    def send_seq(self, value: int) -> None:
        if self._send_sequence is None:
            self._send_sequence = cast(
                Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, value, lock=True)
            )
        else:
            self._send_sequence.value = ctypes.c_uint64(value)

    @property
    def recv_seq(self) -> int:
        return cast(int, self._recv_sequence.value)

    @recv_seq.setter
    def recv_seq(self, value: int) -> None:
        if self._recv_sequence is None:
            self._recv_sequence = cast(
                Synchronized, mp.sharedctypes.Value(ctypes.c_uint64, value, lock=True)
            )
        else:
            self._recv_sequence.value = ctypes.c_uint64(value)

    @property
    def send_next_seq(self) -> int:
        return int(self.send_seq) + 1

    @property
    def recv_next_seq(self) -> int:
        return int(self.recv_seq) + 1

    def send_next_seq_increment(self) -> int:
        """When you want to send a message to the client and you need a
        sequence number to put into a sequenced event/message,
        you call this. It increments and returns the value.
        The value you get back is the one you should use.
        In other words, if you were to break it into steps:
        ```python
        # want to send a message to the client and need a seq Num
        sequence_number_we_need = send_next_seq()
        # we need to update our current sequence number, because
        # we're sending a message
        send_seq_num = sequence_number_we_need
        # and if we were to call next, we'd get
        assert send_next_seq() == sequence_number_we_need + 1

        So, by doing:
        `sequence_number_we_need = send_next_seq_and_increment()`
        it does all of the above.
        ```
        """
        # TODO: check for flow type of None instead of is_client
        if self.is_client:
            # then we are a None Flow and must not increase our sequence
            logging.error(
                f"client={self.is_client} None Flow MUST NOT send a sequence number. Terminate session with protocol error."
            )
            raise ValueError(
                "None Flow MUST NOT send a sequence number. Terminate session with protocol error."
            )
        # logger.debug(f"client={self.is_client} send_next_seq_increment.get_lock()")
        with self._send_sequence.get_lock():
            # logger.debug(f"client={self.is_client} send_next_seq_increment += 1")
            self._send_sequence.value += 1
        # we updated the sequence number so now lets update the last send time
        with self._send_time_last_message.get_lock():
            # while timestamp is a float, it is sufficient for our timer compare
            self._send_time_last_message.value = dt.datetime.now().timestamp()

        # logger.debug("return send_next_seq_increment")
        return int(self._send_sequence.value)

    def recv_next_seq_increment(self) -> int:
        """We are tracking what the next seq should be on the receive
        side of the feed. If we are the server receiving from the client,
        they are a "None" type and shouldn't have any sequence number
        we are expecting, so next_seq will always be 1.
        """
        # TODO: track the Flow type per connection (Idempotent, Recoverable, Unsequenced, None)
        # then check against the appropriate type here instead.
        if not self.is_client:
            logging.error("Cannot increment sequence on a None connection.")
            raise ValueError(
                "Sequenced messages on a None type Flow is a protocol error. Terminate connection."
            )
        with self._recv_sequence.get_lock():
            self._recv_sequence.value += 1
        # logging.debug(
        #     f"client={self.is_client} _recv_sequence.value bumped by 1 now: {self._recv_sequence.value}"
        # )
        # update the last timestamp as we have a new sequence
        with self._recv_time_last_message.get_lock():
            # while timestamp is a float, it's close enough for our needs in a timer
            self._recv_time_last_message.value = dt.datetime.now().timestamp()
        return int(self._recv_sequence.value)

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("Context:\n")
        buf.write(f"    session_id: {self.session_id}\n")
        buf.write(f"    is_client: {self.is_client}\n")
        buf.write(f"    recv_next_seq: {self.recv_next_seq}\n")
        buf.write(f"    send_next_seq: {self.send_next_seq}\n")
        buf.write(f"    session_state: {self.session_state}\n")
        buf.write(f"    recv_time_last_message: {self.recv_time_last_message}\n")
        buf.write(f"    recv_hb_interval_ms: {self.recv_hb_interval_ms}\n")
        buf.write(f"    send_time_last_message: {self.send_time_last_message}\n")
        buf.write(f"    send_hb_interval_ms: {self.send_hb_interval_ms}\n")
        buf.write(f"    credentials: {self.credentials}")
        buf.write(f"    curves_permitted: {self.curves_permitted()}")
        buf.write(f"    predictions_permitted: {self.predictions_permitted()}")
        return buf.getvalue()

    def reset_recv_timer(self) -> None:
        logging.debug(f"client={self.is_client} reset_recv_timer")
        self.cancel_timer(self._recv_hb_timer)
        self._recv_hb_timer = Timer(
            # we're just going to set it for this many seconds in the future
            self.recv_hb_interval_ms / 1000.0,
            self.heartbeat_expired_recv,
        )
        self._recv_hb_timer.start()

    def reset_send_timer(self) -> None:
        # logging.debug(f"client={self.is_client} reset_send_timer")
        self.cancel_timer(self._send_hb_timer)
        self._send_hb_timer = Timer(
            # we're just going to set it for this many seconds in the future
            self.send_hb_interval_ms / 1000.0,
            self.heartbeat_expired_send,
        )
        self._send_hb_timer.start()

    def heartbeat_expired_send(self) -> None:
        """Checks current time vs last send message time
        and if (now - last) // interval_ms > 1 < 4 send
        a heartbeat sequence.
        """
        # then we need to check the last time we sent a message
        # to determine if we need to send a heartbeat or not
        logging.debug(
            f"client={self.is_client} heartbeat_expired_send. call._heartbeat_send_expired_fn()"
        )
        try:
            self._heartbeat_send_expired_fn()
        except EOFError:
            if self.session_state == SessionStateEnum.Terminated:
                logger.info(
                    f"client({self.is_client}) heartbeat send expired returned EOF and {self.session_state} exiting"
                )

    def heartbeat_expired_recv(self) -> None:
        """Checks the recv connection for activity and
        if > 3*heartbeat then self.client_session_dead()

        Means that we need to check if we've received a message
        in the heartbeat limits.
        """
        logging.debug(
            f"client={self.is_client} heartbeat_expired_recv. call._heartbeat_recv_expired_fn()"
        )
        try:
            self._heartbeat_recv_expired_fn()
        except EOFError:
            if self.session_state == SessionStateEnum.Terminated:
                logger.info(
                    f"client({self.is_client}) heartbeat recv expired returned EOF and {self.session_state} exiting"
                )


def initialize() -> None:
    import polars as pl

    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def combined_message_id(template_id: int, schema_id: int, version: int) -> int:
    if not isinstance(template_id, int):
        # logger.debug("template_id type {} converting to int".format(type(template_id)))
        template_id = int(template_id.value)
    if not isinstance(schema_id, int):
        # logger.debug("schema_id type {} converting to int".format(type(schema_id)))
        schema_id = int(schema_id.value)
    if not isinstance(version, int):
        # logger.debug("version type {} converting to int".format(type(version)))
        version = int(version.value)
    """incoming must be the size of the header types, currently all u8."""
    for i in [template_id, schema_id, version]:
        if i < 0 or i > 255:
            raise ValueError(
                "all identifiers must be in the range [0:255]. got: template_id={} schema_id={} version={}".format(
                    template_id, schema_id, version
                )
            )

    retval = template_id
    retval = retval << 8
    retval = retval | schema_id
    retval = retval << 8
    retval = retval | version
    return retval


def combined_message_tuple(combined_message_id: int) -> Tuple[int, int, int]:
    """Takes combined_message_id(int) in range of [0:16777216]
    returns a tuple of components (template_id:int, schema_id:int, version:int)
    """
    # 16777216 is 2**24
    if combined_message_id < 0 or combined_message_id > 16777216:
        raise ValueError("combined_message_id must be in the range [0:16777216]")

    version = combined_message_id & 0x0000_00FF
    combined_message_id >>= 8
    schema_id = combined_message_id & 0x0000_00FF
    combined_message_id >>= 8
    template_id = combined_message_id & 0x0000_00FF

    return (template_id, schema_id, version)


class SplineFixReader:
    """Simple reader that takes a IO like object and turns it into messages to be used."""

    def __init__(
        self,
        is_client: bool = True,
        connection: Optional[Connection] = None,
        initial_heartbeat_ms: int = 3000,
    ) -> None:
        self.registered_mesg_handlers: Dict[int, Callable] = {}
        # just until this is set by something extermally
        if connection is not None:
            self._session_context = SessionContext(conn=connection, is_client=is_client)
        else:
            # won't work, but allows us to create a context to store
            # and we can then set it later outside the program
            c, s = mp_ctx.Pipe()
            self._session_context = SessionContext(conn=s, is_client=is_client)

        # session contexts start with a default heartbeat, so we'll
        # start them here.
        # timers will be started as soon as we set the hb interval
        # default to 3 seconds as a default to give enough time
        # for things to initialize
        self._session_context.recv_hb_interval_ms = initial_heartbeat_ms
        self._session_context.send_hb_interval_ms = initial_heartbeat_ms
        self.session_context._heartbeat_send_expired_fn = self.send_timer_expired
        self.session_context._heartbeat_recv_expired_fn = self.recv_timer_expired

        self.register_handler(
            Sequence._messageType.value,
            HandleHeartbeats.process_sequenced,
            Sequence._schemaId,
            Sequence._version,
        )
        self.register_handler(
            UnsequencedHeartbeat._messageType.value,
            HandleHeartbeats.process_unsequenced,
            UnsequencedHeartbeat._schemaId,
            UnsequencedHeartbeat._version,
        )

    def register_byte_stream(self, socket: Connection) -> None:
        self._session_context._connection = socket

    @property
    def conn(self) -> Connection:
        return self._session_context._connection

    @property
    def session_state(self) -> SessionStateEnum:
        return self._session_context.session_state

    @session_state.setter
    def session_state(self, value: Union[int, SessionStateEnum]) -> None:
        target_value: int
        if isinstance(value, int):
            target_value = int(value)
        elif isinstance(value, SessionStateEnum):
            target_value = int(value.value)
        else:
            raise NotImplementedError
        self._session_context.session_state = SessionStateEnum(target_value)

    @property
    def session_context(self) -> SessionContext:
        return self._session_context

    @session_context.setter
    def session_context(self, context: SessionContext) -> None:
        if not isinstance(context, SessionContext):
            raise NotImplementedError
        self._session_context = context

    def register_handler(
        self, template_id: int, handler: Callable, schema_id: int = 0, version: int = 0
    ) -> None:
        """Allows registration of a single handler for a numeric message id.
        Handler MUST take a bytes object and returns a tuple of
        (handled: bool, remaining_data: bytes)

        raises if another handler is already registered for given message_id/template_id
        """
        combo_msg_id = combined_message_id(template_id, schema_id, version)
        prior = self.registered_mesg_handlers.get(combo_msg_id)
        if prior:
            raise ValueError(
                "another handler is already registered for template_id: {}  handler: {}".format(
                    template_id, prior
                )
            )
        self.registered_mesg_handlers.update({combo_msg_id: handler})

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("SplineFixReader:\n")
        buf.write("    connection: {}\n".format(repr(self.conn)))
        buf.write("    connection.closed: {}\n".format(self.conn.closed))
        buf.write("    connection.readable: {}\n".format(self.conn.readable))
        buf.write("    connection.writable: {}\n".format(self.conn.readable))
        buf.write("    session_context: {}\n".format(self.session_context))
        buf.write("    session_state: {}\n".format(self.session_state))
        buf.write(
            "    registered_mesg_handlers: {}\n".format(self.registered_mesg_handlers)
        )
        return str(buf.getvalue())

    def read_bytes(self, num_bytes: int, conn: Connection) -> bytes:
        """
        logger.error(
            "reader.read_bytes start: {}".format(dt.datetime.now().timestamp())
        )
        """
        buffer = BytesIO()
        total_read = 0
        while (
            total_read < num_bytes and self.session_state != SessionStateEnum.Terminated
        ):
            more_data = conn.poll(0.01)
            if more_data is True:
                if not conn.closed and conn.readable:
                    self.session_context.connection_lock.acquire()
                    try:
                        total_read += buffer.write(conn.recv_bytes())
                    except OSError as ex:
                        logger.warning(
                            f"client({self.session_context.is_client}) reader: read_bytes failed. {ex}"
                        )
                        self.session_state = SessionStateEnum.Terminated
                        break
                    finally:
                        self.session_context.connection_lock.release()

            else:
                # lets test to see if the other end is closed
                if conn.closed or not conn.readable or not conn.writable:
                    logger.info(
                        f"connection is closed: {conn.closed} readable: {conn.readable} writable: {conn.writable}"
                    )
                    self.session_state = SessionStateEnum.Terminated
                    raise EOFError
        if self.session_state == SessionStateEnum.Terminated:
            logger.info(
                "read_bytes session state is Terminated. Clear buffer and return."
            )
            buffer.seek(0, 0)
            buffer.truncate()
            raise EOFError
        """
        logger.error("reader.read_bytes end: {}".format(dt.datetime.now().timestamp()))
        """
        return buffer.getvalue()

    def reader_start(self, stop_at_eof: bool = False) -> None:
        # we've been forked and so we inherit the file descriptors
        # from our parent. One of those is the client end of the pipe
        if self.session_context.other_end_connection is not None:
            self.session_context.other_end_connection.close()
        # start our timers
        self.session_context.reset_recv_timer()
        self.session_context.reset_send_timer()
        buffer = BytesIO()
        sofh_size = Sofh.struct_size_bytes()
        while self.session_state != SessionStateEnum.Terminated:
            try:
                """
                logger.error(
                    "reader.start buffer leftovers: {}".format(
                        dt.datetime.now().timestamp()
                    )
                )
                """
                loc = buffer.tell()
                # seek the end
                buf_end = buffer.seek(0, 2)
                if buf_end == loc:
                    # if we've read all the data
                    """
                    logger.debug(
                        "read all of the data, close() buffer and make a new empty one"
                    )
                    """
                    buffer.close()
                    buffer = BytesIO()
                    loc = 0
                    buf_end = 0
                else:
                    # we're not at the end. Is there another sofh coming up?
                    # NOTE: we started this loop by seeking to end of buffer, so we're still there.
                    # either we just started, and our buffer is empty (then it would have matched)
                    # or we've looped. If we looped, we read out a complete message and should be
                    # at the beginning of the next sofh already

                    # next set of operations check to see if we have enough data to parse again
                    # and will seek to the end and add if not. Since we're already at the end,
                    # we'll leave it there.
                    # buffer.seek(loc, 0)
                    pass
                # we either have a fresh buffer or we have the old one with a sofh header
                """
                logger.error(
                    "reader.end buffer leftovers: {}".format(
                        dt.datetime.now().timestamp()
                    )
                )
                """

                # we already have the loc from above
                # loc = buffer.tell()
                # seek the end to
                # we have this also
                # buf_end = buffer.seek(0, 2)
                while (buf_end - loc) < sofh_size:
                    try:
                        """
                        logger.error(
                            "reader.start write(read_bytes(sofh_size)) : {}".format(
                                dt.datetime.now().timestamp()
                            )
                        )
                        """
                        # want to write at the end, we are a fresh buffer, of we've already gone to the end
                        buf_end += buffer.write(
                            self.read_bytes(sofh_size, conn=self.conn)
                        )
                        """
                        logger.error(
                            "reader.end write(read_bytes(sofh_size)) : {}".format(
                                dt.datetime.now().timestamp()
                            )
                        )
                        """

                    except EOFError:
                        if (
                            stop_at_eof
                            or self.session_state == SessionStateEnum.Terminated
                        ):
                            if not self.conn.closed:
                                try:
                                    self.conn.close()
                                except OSError as ex:
                                    logging.warning(
                                        "usually get EBAD here. {}".format(ex)
                                    )
                            return
                        else:
                            continue
                    except OSError as ex:
                        logger.warning(
                            "Tried to read from closed connection. {}".format(ex)
                        )
                        if not self.conn.closed:
                            self.conn.close()
                        self.session_state = SessionStateEnum.Terminated
                        return
                # set the buffer to the start
                """
                logger.error(
                    "reader.before buffer.seek(loc,0) : {}".format(
                        dt.datetime.now().timestamp()
                    )
                )
                """
                # go to the start of our buffer
                buffer.seek(loc, 0)
                # read out enough for an sofh
                sofh_bytes = buffer.read(sofh_size)
                """
                logger.error(
                    "reader.after buffer.seek({},0) : {}".format(
                        loc, dt.datetime.now().timestamp()
                    )
                )
                """

                if len(sofh_bytes) < sofh_size:
                    # means we didn't get enough bytes in the last read to make a full sofh
                    # seek back to start of sofh and try to get more bytes(we read the bytes so changed our file pointer)
                    buffer.seek(loc, 0)
                    continue
                # have enough to read a sofh
                (sofh, rem_bytes) = Sofh.deserialize(sofh_bytes)
                # next message is in the length field, inclusive of
                # the sofh we've already read, so we'll get the size of just the message body
                msg_body_len = sofh.message_body_len()
                # we've read the sofh already, so we're at the proper location
                # no seek()
                """
                logger.error(
                    "reader.before while get whole message : {}".format(
                        dt.datetime.now().timestamp()
                    )
                )
                """

                # NOTE: based on the message_body_len, we know how many bytes we need to read, but that
                # doesn't mean we have them all.
                loc = buffer.tell()
                end_pos = buffer.seek(0, 2)
                while (
                    msg_body_len > (end_pos - loc)
                    and self.session_state != SessionStateEnum.Terminated
                ):
                    # then we don't have enough to read
                    # and we've already sought to the end of our buffer
                    end_pos += buffer.write(
                        self.read_bytes(msg_body_len - sofh_size, conn=self.conn)
                    )
                    # end_pos = buffer.tell()
                # and set back to after the sofh
                buffer.seek(loc, 0)
                """
                logger.error(
                    "reader.after while get whole message : {}".format(
                        dt.datetime.now().timestamp()
                    )
                )
                """
                if self.session_state == SessionStateEnum.Terminated:
                    return

                # this is most likely the entire message to the tail, but it may be only a portion
                message_bytes = buffer.read(msg_body_len)
                (msg_hdr, rem_bytes) = SplineMessageHeader.deserialize(message_bytes)
                combined_id = combined_message_id(
                    msg_hdr.template_id, msg_hdr.schema_id, msg_hdr.version
                )
                handler = self.registered_mesg_handlers.get(combined_id)
                if handler is None:
                    # then we don't know what to do with this message, just skip it.
                    logger.warning(
                        "received template_id with no registered handler. {}".format(
                            msg_hdr
                        )
                    )
                    logger.warning("bytes:\n{!r}".format(message_bytes.hex(":")))
                    self.send_terminate(
                        data=rem_bytes,
                        context=self.session_context,
                        termination_str=f"client={self.session_context.is_client} received message with no handler",
                    )
                    # buffer is at end of last message and should be at the beginning of the next
                    # discard any rem_bytes
                    continue
                else:
                    handler = cast(Callable, handler)
                # we have a handler, lets call it.
                # since we know we've received a valid message, update our last consumed timestamp
                self.session_context.set_recv_time_last_message(None)
                (handled, rem_bytes) = handler(rem_bytes, self.session_context)
                if handled:
                    # there shouldn't be any bytes left, though there may be padding, as
                    # we read all of the bytes from the sofh header size
                    # so we can discard the rem_bytes
                    # buffer is at the last read pos still
                    continue
                else:
                    # perhaps look up another handler and try again in a loop
                    # TODO: make handlers a dict[int, list[Callable]
                    # when we get a match, go down the list of callables
                    # until one of them handles the message

                    # we don't have that yet
                    logger.warning(
                        "Prior handler did not handle the data.\n{!r}".format(
                            rem_bytes.hex(":")
                        )
                    )
                    # so terminate
                    self.send_terminate(data=rem_bytes, context=self.session_context)
            except Exception as ex:
                logger.error("reader got exception while processing: {}".format(ex))
                self.session_state = SessionStateEnum.Terminated
                if not self.session_context.connection.closed:
                    self.session_context.connection.close()
                raise ex

        logger.info("Session Ended")

    def recv_timer_expired(self) -> None:
        # logging.debug(f"client={self.session_context.is_client} recv_timer_expired")
        timestamp_now: float = dt.datetime.now().timestamp()
        time_since_last_ms: float
        last_message_timestamp: float
        hb_ms: int
        # check to see if there has been traffic
        context = self.session_context
        last_message_timestamp = context.recv_time_last_message_timestamp

        # integers are seconds and float is nanoseconds
        time_since_last_ms = (timestamp_now - last_message_timestamp) * 1_000
        time_since_last_ms = max(0, time_since_last_ms)
        logging.debug(
            f"client({self.session_context.is_client}) timesince_last_ms = (timestamp_now - last_message_timestamp)\
             * 1000 == {time_since_last_ms} = ({timestamp_now} - {last_message_timestamp}) * 1000)"
        )
        time_since_last_ms = max(0, time_since_last_ms)
        hb_ms = context.recv_hb_interval_ms
        logging.debug(
            f"client({context.is_client}) time_since_last_ms={time_since_last_ms}"
        )

        if time_since_last_ms > hb_ms:
            logging.debug(
                f"client({self.session_context.is_client}) time_since_last_ms > hb_ms"
            )
            # then we'll take some action.
            excessive_missed_heartbeats = 10
            if time_since_last_ms > (hb_ms * excessive_missed_heartbeats):
                # that's more than enough
                raise Exception(
                    f"missed more than {excessive_missed_heartbeats} receive heartbeats. we're done here"
                )
            if time_since_last_ms > (hb_ms * 3):
                logging.debug(
                    f"client({context.is_client}) missed more than 3 heartbeat intervals time_since_last_msg={time_since_last_ms} > hb_ms={hb_ms} * 3"
                )
                if context.session_state == SessionStateEnum.Terminating:
                    if not context.connection.closed:
                        logging.debug(
                            f"client={context.is_client} sent terminate and have waited more than 3 hb. connection is not closed, closing."
                        )
                        context.connection.close()
                    context.session_state = SessionStateEnum.Terminated
                    # don't reset the timer
                    return
                elif context.session_state == SessionStateEnum.Terminated:
                    logging.debug(
                        f"client({context.is_client}) 3 * hb interval have expired and we're already in a Terminated state. Raising EOFError."
                    )
                    # TODO: Raise EOFError or exit cleanly?
                    # raise EOFError
                    return

                # if we've missed at least 3 hearbeats
                # not receiving messages from the server, terminate
                # the connection
                if not context.connection.closed:
                    logging.debug(
                        f"client={context.is_client} connection is not closed, sending terminate"
                    )
                    self.send_terminate(
                        data=bytes(),
                        context=context,
                        termination_code=TerminationCodeEnum.Unspecified,
                    )
                    # since we may have connectivity problems. Disconnect
                    # after send
                    context.connection.close()
            # else:
            #   it hasn't been 3 heartbeats yet, so just wait a bit
            #   as we're receiving, there is nothing for us to send
            logging.debug(
                f"client({self.session_context.is_client}) recv timer fired and was > hb_ms but less than 3 * hb_ms. Just reset timer"
            )

        # reset the timer
        context.reset_recv_timer()

    def send_timer_expired(self) -> None:
        logging.debug(f"client={self.session_context.is_client} send_timer_expired")
        # when was the last message?
        timestamp_now = dt.datetime.now().timestamp()
        context = self.session_context
        last_message_timestamp = context.send_time_last_message_timestamp
        # integers are seconds and float is nanoseconds
        time_since_last_ms = (timestamp_now - last_message_timestamp) * 1_000
        time_since_last_ms = max(0, time_since_last_ms)
        logging.debug(
            f"client({self.session_context.is_client}) time_since_last_ms={time_since_last_ms}"
        )
        hb_ms = context.send_hb_interval_ms
        if time_since_last_ms > hb_ms:
            logging.debug(f"client({context.is_client}) time_since_last_ms > hb_ms")
            # then we'll take some action.
            if time_since_last_ms > (hb_ms * 3):
                logging.debug(
                    f"client={context.is_client} missed more than 3 heartbeat intervals time_since_last_msg={time_since_last_ms} > hb_ms={hb_ms} * 3"
                )
                # if we've missed at least 3 hearbeats, which is weird,
                # because this should be based off our send time, and we
                # are responsible for that by sending a heartbeat.
                # Something is not right, terminate the connection
                if not context.connection.closed:
                    logging.debug(
                        f"client={context.is_client} no messages sent in 3 hb intervals, sending a terminate message"
                    )
                    self.send_terminate(
                        data=bytes(),
                        context=context,
                        termination_code=TerminationCodeEnum.Unspecified,
                    )
                    # since we may have connectivity problems. Disconnect
                    # after send
                    logging.debug(
                        f"client={context.is_client} and then closing the connection"
                    )
                    context.connection.close()
                    logging.debug(f"client={context.is_client} closed the connection")

                # whether or not we've closed the connection, make sure
                # we're in a terminate state.
                logging.debug(
                    f"client={context.is_client} set session_state to Terminated"
                )
                self.session_state = SessionStateEnum.Terminated
                logging.debug(
                    f"client={context.is_client} session_state has been set to Terminated"
                )

                # don't reset the timer
                logger.info(
                    f"client({self.session_context.is_client}) 3rd timer expired. Declaring self terminated"
                )
                logging.debug(f"client={context.is_client} raise EOFError")
                # won't be caught or show up because we're called from a timer
                raise EOFError
            elif self.session_state == SessionStateEnum.Established:
                logging.debug(
                    f"client({context.is_client}) time since last send > hb_ms < 3*hb_ms, send seq/hb msg"
                )
                # it hasn't been 3 heartbeats yet
                # but we haven't sent any info in a heartbeat interval,
                # so let the other side know we're still alive.
                if context.is_client:
                    # client is a "None" type, so we send
                    # an unsequenced heartbeat
                    logging.debug(
                        f"client={self.session_context.is_client} send_timer fired, sending heartbeat_message"
                    )
                    try:
                        self.send_heartbeat_message()
                    except Exception as ex:
                        logging.error(
                            "exception trying to call self.send_heartbeat_message(). Ex: {}".format(
                                ex
                            )
                        )
                else:
                    # we're the server and Idempotent type,
                    # so we send a sequence message
                    logging.debug(
                        f"client={self.session_context.is_client} send_timer fired, sending sequence_message"
                    )
                    self.send_sequence_message()
            elif self.session_state == SessionStateEnum.Establishing:
                # we aren't over 3 * the interval, but haven't established
                # our session yet. Just reset.
                logging.debug(
                    f"client={context.is_client} heartbeat timer expired before session established. Resetting timer."
                )
            else:
                # we aren't over 3 * the interval, but haven't established, or finalized
                # our session yet. Just reset.
                logging.debug(
                    f"client={context.is_client} heartbeat timer expired while session_state={context.session_state}. Resetting timer."
                )
        # either way, make sure that the current timer is canceled
        # and start a new one
        logging.debug(f"client({context.is_client}) reset_send_timer")
        context.reset_send_timer()

    def send_sequence_message(self) -> None:
        """Sent on Idempotentent and recoverable negotiated sessions
        as a keep alive.
        Also, only send a sequence or unsequenced heartbeat on the
        send connection.
        """
        HandleHeartbeats.send_sequenced(data=bytes(), context=self.session_context)

    def send_heartbeat_message(self) -> None:
        """Sent when sender has negotiated a "None" or "Unsequenced" connection
        UnsequencedHeartbeat and Sequence messages are only sent on the "send"
        connection.
        """
        logging.debug(
            f"client({self.session_context.is_client} entered send_heartbeat_message)"
        )
        HandleHeartbeats.send_unsequenced(data=bytes(), context=self.session_context)
        logging.debug(
            f"client({self.session_context.is_client} exiting send_heartbeat_message)"
        )

    def register_send_terminate(self, fn: Callable) -> None:
        self.send_terminate = fn


class HandleHeartbeats:
    @staticmethod
    def send_unsequenced(
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        logging.debug(f"client({context.is_client}) send_unsequenced called")
        unseq_msg = UnsequencedHeartbeat()
        unseq_bytes = generate_message_from_type(unseq_msg)
        context.connection_lock.acquire()
        try:
            context.connection.send_bytes(unseq_bytes)
        finally:
            context.connection_lock.release()
        SessionContext.set_send_time_last_message(self=context, value=None)
        context.send_time_last_message = None  # type: ignore[assignment]
        logging.debug(
            f"client({context.is_client}) sent UnsequencedHeartbeat({unseq_msg}) message to connection"
        )
        return (True, data)

    @staticmethod
    def send_sequenced(
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        sequence: int
        sequence = context.send_next_seq

        seq_msg = Sequence(next_sequence_number=sequence)
        seq_bytes = generate_message_from_type(seq_msg)
        context.connection_lock.acquire()
        try:
            context.connection.send_bytes(seq_bytes)
        finally:
            context.connection_lock.release()
        context.set_send_time_last_message(None)
        logging.debug(f"client={context.is_client} sent sequence message to connection")
        return (True, data)

    @staticmethod
    def process_sequenced(
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        logging.debug(f"client={context.is_client}.process_sequenced called")
        # are we in an established connection?
        if context.session_state != SessionStateEnum.Established:
            # protocol error, and aren't established, terminate
            logging.error("Got sequenced heartbeat while not established. Terminating.")
            context.session_state = SessionStateEnum.Terminate
            return (False, data)
        # TODO: check for None, or Unsequenced vs is_client
        if not context.is_client:
            # shouldn't be getting a sequenced message from a None
            # Flow type. It is a protocol error.
            logging.error(
                "Got sequenced heartbeat from a None Flow type. Terminating due to protocol error."
            )
            context.session_state = SessionStateEnum.Terminate
            return (False, data)
        # else in the established state and expecting a sequence from
        # an Idempotent or Recoverable Flow
        # Last recv message time will have been updated already, so we
        # just need to check the sequence number to see if we've missed any messages
        (seq_msg, rem_bytes) = Sequence.deserialize(data=data)
        seq_msg = cast(Sequence, seq_msg)
        next_recv_seq = seq_msg.next_seq_num
        if next_recv_seq != context.recv_next_seq:
            logging.warning(
                f"client={context.is_client} sequence number mismatch. Seq.next_seq_num={next_recv_seq} context.recv_next_seq={context.recv_next_seq}"
            )
        # TODO: if we were to do gap request, it would go here
        return (True, rem_bytes)

    @staticmethod
    def process_unsequenced(
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        logging.debug(f"client={context.is_client}.process_unsequenced called")
        # are we in an established connection?
        if context.session_state != SessionStateEnum.Established:
            # protocol error, and aren't established, terminate
            logging.error(
                "Got unsequenced heartbeat while not established. Terminating."
            )
            context.session_state = SessionStateEnum.Terminate
            return (False, data)
        # TODO: check for Idempotent, or Recoverable vs is_client
        if context.is_client:
            # shouldn't be getting an unsequenced message from an Idempotent
            # Flow type. It is a protocol error.
            logging.error(
                "Got unsequenced heartbeat from a Idempotent Flow type. Terminating due to protocol error."
            )
            context.session_state = SessionStateEnum.Terminate
            return (False, data)
        # else in the established state and expecting an unsequenced heartbeat from
        # a None or Unsequenced Flow
        # Last recv message time will have been updated already, so nothing else
        # to do
        (unseq_msg, rem_bytes) = UnsequencedHeartbeat.deserialize(data)
        unseq_msg = cast(UnsequencedHeartbeat, unseq_msg)
        logging.debug(f"client={context.is_client} Unsequenced.\n{unseq_msg}")
        return (True, rem_bytes)
