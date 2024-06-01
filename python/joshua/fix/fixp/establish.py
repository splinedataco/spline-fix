try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any, cast, Optional, Tuple, Union
from io import StringIO
import struct
import logging
import datetime as dt
from joshua.fix.types import (
    SplineByteData,
    SplineDateTime,
    SplineNanoTime,
)
from joshua.fix.messages import SplineFixType
from joshua.fix.data_dictionary import (
    SPLINE_MUNI_DATA_SCHEMA_VERSION,
    SPLINE_FIXP_SESSION_SCHEMA_ID,
)
from joshua.fix.common import (
    align_by,
    FixpMessageType,
)
from joshua.fix.fixp.messages import Jwt, Uuid
from enum import Enum


class EstablishmentRejectCodeEnum(Enum):
    Unspecified = 0
    Unnegotiated = 1
    AlreadyEstablished = 2
    SessionBlocked = 3
    KeepaliveInterval = 4
    Credentials = 5


REJECTION_REASON_STRINGS = [
    "Unspecified:",
    "Unnegotiated: Establish request was not preceded by a Negotiation or session was finalized, requiring renegotiation",
    "AlreadyEstablished: EstablishmentAck was already sent; Establish was redundant",
    "SessionBlocked: user is not authorized",
    "KeepaliveInterval: value is out of accepted range",
    "Credentials: failed because identity is not recognized, or user is not authorized to use service",
]


class Establish(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.Establish
    _sessionId: Uuid
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _timestamp_nano: SplineNanoTime
    # u32
    _keep_alive_interval_ms: int
    # max 10 seconds, min 100ms
    _keep_alive_min_max: Tuple[int, int] = (100, 10_000)
    # u64
    _next_seq_num: int
    # group
    _credentials: Optional[Jwt]
    # fixed size fields only, no groups
    _blockLength: int
    # 16 bytes
    _blockLength = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _blockLength += SplineNanoTime.struct_size_bytes()
    # keepAliveInterval: DeltaMillisecs
    # 4 bytes
    _blockLength += 4
    # u64, 8 bytes
    _blockLength += 8
    # will want padding before group
    _group_pad = align_by(_blockLength)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _blockLength += _group_pad

    # credentials are a group, and aren't included in blockLength

    # schema_id, version, and template_id are all needed only when static
    # but after >=3.11 no longer supports both @classmethod and @property
    # so making them all functions
    @property
    def template_id(self) -> int:
        return Establish._messageType.value

    @property
    def schema_id(self) -> int:
        return Establish._schemaId

    @property
    def version(self) -> int:
        return Establish._version

    @property
    def timestamp(self) -> SplineNanoTime:
        return self._timestamp_nano

    @property
    def blockLength(self) -> int:
        return self._blockLength

    @property
    def next_seq_num(self) -> int:
        return self._next_seq_num

    @next_seq_num.setter
    def next_seq_num(self, value: int) -> None:
        if not isinstance(value, int):
            raise NotImplementedError
        if value < 0:
            raise ValueError("sequence number must be positive")
        self._next_seq_num = value

    def get_seq_num(self) -> int:
        return self.increment_seq_num()

    def increment_seq_num(self) -> int:
        """Increments the sequence number and returns the prior value."""
        prior_value = self._next_seq_num
        self._next_seq_num += self._next_seq_num
        return prior_value

    def __init__(
        self,
        credentials: Optional[Union[Jwt, bytes]] = None,
        uuid: Optional[Uuid] = None,
        timestamp: Optional[
            Union[dt.datetime, SplineDateTime, SplineNanoTime, float]
        ] = None,
        keep_alive_ms: Optional[int] = 1_000,  # 1 sec default, max 10s
        next_sequence_num: int = 1,
    ) -> None:
        logging.debug(
            "Establish(credentials: {!r}, uuid: {}, timestamp: {}, keep_alive_ms: {})".format(
                credentials, uuid, timestamp, keep_alive_ms
            )
        )
        # force validation
        logging.debug("type(uuid)={}".format(type(uuid)))
        self.sessionId = uuid  # type: ignore[assignment]
        if credentials is not None:
            if isinstance(credentials, bytes):
                credentials = Jwt(token=credentials)
            if isinstance(credentials, Jwt):
                logging.debug(
                    "Establish.__init__ got Jwt. Assigning to credentials. {}".format(
                        credentials
                    )
                )
                self._credentials = credentials
            else:
                raise NotImplementedError
        else:
            self._credentials = None

        self.timestamp_nano = timestamp  # type: ignore[assignment]
        self.keep_alive = keep_alive_ms  # type: ignore[assignment]
        self.next_seq_num = next_sequence_num

    @property
    def credentials(self) -> Optional[Jwt]:
        return self._credentials

    @credentials.setter
    def credentials(self, cred: Optional[Union[Jwt, bytes]] = None) -> None:
        if cred is None:
            self._credentials = None
            return
        elif isinstance(cred, bytes):
            cred = Jwt(token=cred)
        if isinstance(cred, Jwt):
            logging.debug(
                "Establish.__init__ got Jwt. Assigning to credentials. {}".format(cred)
            )
            self._credentials = cred
        else:
            raise NotImplementedError

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            raise NotImplementedError

    @property
    def timestamp_nano(self) -> SplineNanoTime:
        return self._timestamp_nano

    @timestamp_nano.setter
    def timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "timestamp_nano: type(tmstmp)={} value={}".format(type(tmstamp), tmstamp)
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._timestamp_nano = value

    @property
    def keep_alive(self) -> int:
        return self._keep_alive_interval_ms

    @keep_alive.setter
    def keep_alive(self, value: Any) -> None:
        if not isinstance(value, int):
            raise NotImplementedError
        if value < self._keep_alive_min_max[0] or value > self._keep_alive_min_max[1]:
            raise ValueError(
                f"keep_alive_ms must be in range [{self._keep_alive_min_max[0]}:{self._keep_alive_min_max[1]}]"
            )
        self._keep_alive_interval_ms = value

    def serialize(self) -> bytes:
        # jwt needs to come last because it's a "group"
        # which has varying length
        # 16B = 16B
        ret_buf = self._sessionId.serialize()
        # 8B  = 24B
        ret_buf += self._timestamp_nano.serialize()
        # 4B  = 28B
        ret_buf += struct.pack("<L", self._keep_alive_interval_ms)
        # 8B  = 36B
        ret_buf += struct.pack("<Q", self._next_seq_num)
        # 36//4 = 0 padding is 0
        #       = 36B
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        if self._credentials is not None:
            # 4 bytes for group, then bytes for bytearray.
            ret_buf += self._credentials.serialize()
        else:
            # put an empty group
            # 4 bytes, empty group
            # 40B total
            ret_buf += SplineByteData(data=bytes()).serialize()
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "Establish.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )

        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 8 bytes
        (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
        # 1 byte
        (keep_alive_ms_int,) = struct.unpack("<L", remaining_bytes[0:4])
        remaining_bytes = remaining_bytes[4:]
        (next_seq_num_int,) = struct.unpack("<Q", remaining_bytes[:8])
        remaining_bytes = remaining_bytes[8:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        # don't have a real Jwt in there yet, just deserialize as a group
        (cred, remaining_bytes) = SplineByteData.deserialize(remaining_bytes, bytes)
        # when we have a credential here we'll want to test here
        if len(cred) == 0:
            cred = None  # type: ignore[assignment]

        new_self = cls(
            credentials=cred,  # type: ignore[arg-type]
            uuid=cast(Uuid, sessionId),
            timestamp=cast(SplineNanoTime, timestamp),
            keep_alive_ms=keep_alive_ms_int,
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._blockLength

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write("  timestamp_nano: {}".format(self._timestamp_nano))
        str_buf.write(
            "  keep_alive_interval_ms: {}".format(self._keep_alive_interval_ms)
        )
        str_buf.write("  next_sequence_num: {}".format(self._next_seq_num))
        str_buf.write("  credentials: {}".format(self.credentials))
        str_buf.write("  blockLength: {}".format(self._blockLength))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.timestamp_nano != value.timestamp_nano:
            logging.debug("{} != {}".format(self.timestamp_nano, value.timestamp_nano))
            return False
        if self._keep_alive_interval_ms != value._keep_alive_interval_ms:
            logging.debug(
                "{} != {}".format(
                    self._keep_alive_interval_ms, value._keep_alive_interval_ms
                )
            )
            return False
        if self._next_seq_num != value._next_seq_num:
            logging.debug("{} != {}".format(self._next_seq_num, value._next_seq_num))
            return False
        if self._credentials != value._credentials:
            logging.debug("{} != {}".format(self._credentials, value._credentials))
            return False
        return True


class EstablishmentReject(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.EstablishmentReject
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _sessionId: Uuid
    _request_timestamp_nano: SplineNanoTime
    _reject_code: EstablishmentRejectCodeEnum
    _reject_str: SplineByteData
    # fixed size fields only, no groups
    _blockLength: int
    # 16 bytes
    _blockLength = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _blockLength += SplineNanoTime.struct_size_bytes()
    # EstablishmentRejectCodeEnum
    # 1 byte
    _blockLength += 1
    # will want padding before group
    _group_pad = align_by(_blockLength)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _blockLength += _group_pad

    @property
    def reject_str(self) -> str:
        return self._reject_str.data.decode("latin1")

    @property
    def reject_code(self) -> EstablishmentRejectCodeEnum:
        return self._reject_code

    @reject_code.setter
    def reject_code(self, value: Union[EstablishmentRejectCodeEnum, int]) -> None:
        target_value: EstablishmentRejectCodeEnum
        if isinstance(value, int):
            target_value = EstablishmentRejectCodeEnum(value)
        elif isinstance(value, EstablishmentRejectCodeEnum):
            target_value = value
        else:
            raise NotImplementedError
        self._reject_code = target_value

    @property
    def blockLength(self) -> int:
        return self._blockLength

    # Reason string is a group, and groups aren't included in blockLength
    def __init__(
        self,
        session_id: Uuid,
        request_timestamp: Union[dt.datetime, SplineDateTime, SplineNanoTime, float],
        reject_code: EstablishmentRejectCodeEnum = EstablishmentRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
    ) -> None:
        logging.debug(
            "EstablishmentReject(session_id: {}, request_timestamp: {}, reject_code: {}, reject_string: {})".format(
                session_id,
                request_timestamp,
                reject_code,
                REJECTION_REASON_STRINGS[reject_code.value],
            )
        )
        # force validation
        logging.debug("type(uuid)={}".format(type(session_id)))
        self.sessionId = session_id

        self.request_timestamp_nano = request_timestamp  # type: ignore[assignment]

        if not isinstance(reject_code, EstablishmentRejectCodeEnum):
            raise NotImplementedError
        self._reject_code = reject_code

        if reject_str is None:
            reject_str = REJECTION_REASON_STRINGS[reject_code.value]
            self._reject_str = SplineByteData(data=reject_str.encode("latin-1"))
        elif isinstance(reject_str, str):
            if self._reject_code != 0:
                logging.debug(
                    "Unspecified reject str may only be specified when reject code is {}".format(
                        EstablishmentRejectCodeEnum.Unspecified
                    )
                )
            # prepend - Unspecified:
            if not reject_str.startswith(REJECTION_REASON_STRINGS[reject_code.value]):
                reject_str = REJECTION_REASON_STRINGS[reject_code.value] + reject_str

            # turn str into bytes
            reject_bytes = reject_str.encode("latin-1")
            self._reject_str = SplineByteData(data=reject_bytes)

    @property
    def template_id(self) -> int:
        return EstablishmentReject._messageType.value

    @property
    def schema_id(self) -> int:
        return EstablishmentReject._schemaId

    @property
    def version(self) -> int:
        return EstablishmentReject._version

    @classmethod
    def from_establish(
        cls,
        est: Establish,
        reject_code: Optional[
            EstablishmentRejectCodeEnum
        ] = EstablishmentRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
    ) -> Self:
        if not isinstance(est, Establish):
            raise NotImplementedError
        return cls(
            session_id=est.sessionId,
            request_timestamp=est.timestamp_nano,
            reject_code=cast(EstablishmentRejectCodeEnum, reject_code),
            reject_str=reject_str,
        )

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            logging.debug(f"sessionId= got type {type(value)}")
            raise NotImplementedError

    @property
    def request_timestamp_nano(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    @request_timestamp_nano.setter
    def request_timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "request_timestamp_nano: type(tmstmp)={} value={}".format(
                type(tmstamp), tmstamp
            )
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._request_timestamp_nano = value

    def serialize(self) -> bytes:
        # jwt/data needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += self._request_timestamp_nano.serialize()
        ret_buf += struct.pack("<B", self._reject_code.value)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        ret_buf += self._reject_str.serialize()
        logging.debug(
            "serialize just reject reason str:\n{!r} : {}".format(
                ret_buf[self.struct_size_bytes() :].hex(":"),
                self._reject_str.data.decode("latin-1"),
            )
        )
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Any, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "EstablishmentReject.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )
        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 8 bytes
        (timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
        # 1 byte
        (reject_code_int,) = struct.unpack("<B", remaining_bytes[0:1])
        reject_code = EstablishmentRejectCodeEnum(reject_code_int)
        remaining_bytes = remaining_bytes[1:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        (reject_str, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes, group_type=bytes
        )

        new_self = cls(
            session_id=cast(Uuid, sessionId),
            request_timestamp=cast(SplineNanoTime, timestamp),
            reject_code=reject_code,
            reject_str=cast(bytes, reject_str).decode("latin-1"),
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._blockLength

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write(
            "  request_timestamp_nano: {}".format(self._request_timestamp_nano)
        )
        str_buf.write("  reject_code: {}".format(self._reject_code))
        str_buf.write("  Reason: {}".format(self._reject_str))
        str_buf.write("  blockLength: {}".format(self._blockLength))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        logging.debug("EstablishReject: self={}\nother={}".format(self, value))
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.request_timestamp_nano != value.request_timestamp_nano:
            logging.debug(
                "{} != {}".format(
                    self.request_timestamp_nano, value.request_timestamp_nano
                )
            )
            return False
        if self._reject_code != value._reject_code:
            logging.debug("{} != {}".format(self._reject_code, value._reject_code))
            return False
        if self._reject_str != value._reject_str:
            logging.debug("{} != {}".format(self._reject_str, value._reject_str))
            return False
        return True


class EstablishmentAck(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.EstablishmentAck
    _sessionId: Uuid
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _request_timestamp_nano: SplineNanoTime
    # u32
    _keep_alive_interval_ms: int
    # max 10 seconds, min 100ms
    _keep_alive_min_max: Tuple[int, int] = (100, 10_000)

    # fixed size fields only, no groups
    _blockLength: int
    # 16 bytes
    _blockLength = Uuid.struct_size_bytes()
    # u64, 8 bytes
    _blockLength += SplineNanoTime.struct_size_bytes()
    # u32, 4 bytes for keep alive interval
    _blockLength += 4
    # u64, 8 bytes for next seq number
    _server_seq_num: int = 0
    # no group so won't pad
    _group_pad = 0
    # we add it to the blockLength so that we don't
    # try to align it again when using struct_size_bytes()
    # (it's part of the root block size and comes after the root
    # and before any groups)
    _blockLength += _group_pad
    _num_groups = 1

    @property
    def num_groups(self) -> int:
        return 1

    # adding these as aliases
    @property
    def timestamp(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    @property
    def request_timestamp(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    def __init__(
        self,
        session_id: Uuid,
        request_timestamp: Union[dt.datetime, SplineDateTime, SplineNanoTime, float],
        keep_alive_ms: int,
        server_sequence_num: int = 0,
    ) -> None:
        logging.debug(
            "EstablishmentAck(session_id: {}, request_timestamp: {}, keep_alive_interval_ms: {}, server_sequence_num: {})".format(
                session_id, request_timestamp, keep_alive_ms, server_sequence_num
            )
        )
        # force validation
        logging.debug("type(session_id)={}".format(type(session_id)))
        self.sessionId = session_id
        self.request_timestamp_nano = request_timestamp  # type: ignore[assignment]
        self.keep_alive = keep_alive_ms
        self._server_seq_num = server_sequence_num

    @property
    def template_id(self) -> int:
        return EstablishmentAck._messageType.value

    @property
    def schema_id(self) -> int:
        return EstablishmentAck._schemaId

    @property
    def version(self) -> int:
        return EstablishmentAck._version

    @property
    def blockLength(self) -> int:
        return self._blockLength

    @property
    def next_seq_num(self) -> int:
        return self._server_seq_num + 1

    @property
    def seq_num(self) -> int:
        return self._server_seq_num

    @seq_num.setter
    def seq_num(self, value: int) -> None:
        if not isinstance(value, int):
            raise NotImplementedError
        if value < 0:
            raise ValueError("sequence number must be positive")
        self._server_seq_num = value

    def get_seq_num(self) -> int:
        return self.increment_seq_num()

    def increment_seq_num(self) -> int:
        """Increments the sequence number and returns the new seq num."""
        self._server_seq_num += 1
        return self._server_seq_num

    @classmethod
    def from_establish(cls, est: Establish, server_seq_num: int) -> Self:
        if not isinstance(est, Establish):
            raise NotImplementedError
        return cls(
            session_id=est.sessionId,
            request_timestamp=est.timestamp_nano,
            keep_alive_ms=est.keep_alive,
            # this would be replaced by the
            # sessions server side sequence
            server_sequence_num=server_seq_num,
        )

    @property
    def sessionId(self) -> Uuid:
        return self._sessionId

    @sessionId.setter
    def sessionId(self, value: Optional[Uuid] = None) -> None:
        logging.debug("sessionId.setter({})".format(value))
        if value is None:
            value = Uuid()
        if isinstance(value, Uuid):
            self._sessionId = value
        else:
            raise NotImplementedError

    @property
    def request_timestamp_nano(self) -> SplineNanoTime:
        return self._request_timestamp_nano

    @request_timestamp_nano.setter
    def request_timestamp_nano(
        self,
        tmstamp: Optional[
            Union[SplineNanoTime, SplineDateTime, dt.datetime, float]
        ] = None,
    ) -> None:
        logging.debug(
            "request_timestamp_nano: type(tmstmp)={} value={}".format(
                type(tmstamp), tmstamp
            )
        )
        value: SplineNanoTime
        if tmstamp is None:
            tmstamp = dt.datetime.now()
        if isinstance(tmstamp, float):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, dt.datetime):
            value = SplineNanoTime(other=tmstamp)
        elif isinstance(tmstamp, SplineDateTime):
            value = SplineNanoTime(other=tmstamp.timestamp)
        elif isinstance(tmstamp, SplineNanoTime):
            value = tmstamp
        else:
            raise NotImplementedError

        self._request_timestamp_nano = value

    @property
    def keep_alive(self) -> int:
        return self._keep_alive_interval_ms

    @keep_alive.setter
    def keep_alive(self, value: Any) -> None:
        if not isinstance(value, int):
            raise NotImplementedError
        if value < self._keep_alive_min_max[0] or value > self._keep_alive_min_max[1]:
            raise ValueError(
                f"keep_alive_ms must be in range [{self._keep_alive_min_max[0]}:{self._keep_alive_min_max[1]}]"
            )
        self._keep_alive_interval_ms = value

    def serialize(self) -> bytes:
        # jwt/data needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += self._request_timestamp_nano.serialize()
        ret_buf += struct.pack("<L", self._keep_alive_interval_ms)
        # next sequence number
        ret_buf += struct.pack("<Q", self.next_seq_num)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        # shouldn't be any padding. Here for consistency.
        ret_buf += b"\x00" * padding
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "EstablishmentAck.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )
        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        sessionId = cast(Uuid, sessionId)
        # 8 bytes
        (request_timestamp, remaining_bytes) = SplineNanoTime.deserialize(
            remaining_bytes
        )
        (keep_alive_int,) = struct.unpack("<L", remaining_bytes[0:4])
        remaining_bytes = remaining_bytes[4:]
        (next_seq_int,) = struct.unpack("<Q", remaining_bytes[0:8])
        remaining_bytes = remaining_bytes[8:]

        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]

        new_self = cls(
            session_id=cast(Uuid, sessionId),
            request_timestamp=cast(SplineNanoTime, request_timestamp),
            keep_alive_ms=keep_alive_int,
            server_sequence_num=next_seq_int - 1,
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._blockLength

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write(
            "  request_timestamp_nano: {}".format(self._request_timestamp_nano)
        )
        str_buf.write("  next_sequence_num: {}".format(self.next_seq_num))
        str_buf.write("  blockLength: {}".format(self._blockLength))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        logging.debug("EstablishmentAck: self={}\nother={}".format(self, value))
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self.request_timestamp_nano != value.request_timestamp_nano:
            logging.debug(
                "{} != {}".format(
                    self.request_timestamp_nano, value.request_timestamp_nano
                )
            )
            return False
        if self.next_seq_num != value.next_seq_num:
            logging.debug("{} != {}".format(self.next_seq_num, value.next_seq_num))
            return False

        return True
