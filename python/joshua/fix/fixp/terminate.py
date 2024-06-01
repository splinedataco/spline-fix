from typing import Any, cast, Optional, Union
from io import StringIO
import struct
import logging
from joshua.fix.types import (
    SplineByteData,
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
from joshua.fix.fixp.messages import Uuid
from enum import Enum


class TerminationCodeEnum(Enum):
    Unspecified = 0
    Finished = 1
    ReRequestOutOfBounds = 2
    ReRequestInProgress = 3


TERMINATE_REASON_STRINGS = [
    "UnspecifiedError: ",
    "Finished: ",
    "ReRequestOutOfBounds: ",
    "ReRequestInProgress: ",
]


class Terminate(SplineFixType):
    _messageType: FixpMessageType = FixpMessageType.Terminate
    _sessionId: Uuid
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    # fixed size fields only, no groups
    _block_length: int
    # 16 bytes
    _block_length = Uuid.struct_size_bytes()
    # 1 byte
    _termination_code: TerminationCodeEnum
    _block_length += 1
    # will want padding before end
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad

    # need one group for the potential reason string
    # though the group size is not included in the root block length
    _group_num: int = 1
    _termination_str: SplineByteData

    @property
    def block_length(self) -> int:
        return self._block_length

    @property
    def termination_str(self) -> str:
        return self._termination_str.data.decode("latin1")

    @property
    def termination_code(self) -> TerminationCodeEnum:
        return self._termination_code

    @termination_code.setter
    def termination_code(self, value: Union[TerminationCodeEnum, int]) -> None:
        target_value: TerminationCodeEnum
        if isinstance(value, int):
            target_value = TerminationCodeEnum(value)
        elif isinstance(value, TerminationCodeEnum):
            target_value = value
        else:
            raise NotImplementedError
        self._termination_code = target_value

    # schema_id, version, and template_id are all needed only when static
    # but after >=3.11 no longer supports both @classmethod and @property
    # so making them all functions
    @property
    def template_id(self) -> int:
        return Terminate._messageType.value

    @property
    def schema_id(self) -> int:
        return Terminate._schemaId

    @property
    def version(self) -> int:
        return Terminate._version

    @property
    def blockLength(self) -> int:
        return self._block_length

    # Reason string is a group, and groups aren't included in blockLength
    def __init__(
        self,
        session_id: Uuid,
        termination_code: TerminationCodeEnum = TerminationCodeEnum.Unspecified,
        termination_str: Optional[str] = None,
    ) -> None:
        if not isinstance(termination_code, TerminationCodeEnum):
            termination_code = TerminationCodeEnum(int(termination_code))
        logging.debug(
            "Terminate(session_id: {}, termination_code: {}, termination_string: {})".format(
                session_id,
                termination_code,
                termination_str,
            )
        )
        self._termination_str = SplineByteData(data=bytes())
        # force validation
        logging.debug("type(uuid)={}".format(type(session_id)))
        self.sessionId = session_id

        if not isinstance(termination_code, TerminationCodeEnum):
            raise NotImplementedError
        self._termination_code = termination_code

        if termination_str is None:
            termination_str = TERMINATE_REASON_STRINGS[termination_code.value]
            self._termination_str = SplineByteData(
                data=termination_str.encode("latin-1")
            )
        elif isinstance(termination_str, str):
            # prepend - Unspecified:
            if not termination_str.startswith(
                TERMINATE_REASON_STRINGS[termination_code.value]
            ):
                termination_str = (
                    TERMINATE_REASON_STRINGS[termination_code.value] + termination_str
                )

            # turn str into bytes
            terminate_bytes = termination_str.encode("latin-1")
            self._termination_str = SplineByteData(data=terminate_bytes)

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

    def serialize(self) -> bytes:
        # data needs to come last because it's a "group"
        # which has varying length
        ret_buf = self._sessionId.serialize()
        ret_buf += struct.pack("<B", self._termination_code.value)
        padding = self._group_pad
        logging.debug("serialize: padding is {}".format(padding))
        ret_buf += b"\x00" * padding
        ret_buf += self._termination_str.serialize()
        logging.debug(
            "serialize just termination reason str:\n{!r} : {}".format(
                ret_buf[self.struct_size_bytes() :].hex(":"),
                self._termination_str.data.decode("latin-1"),
            )
        )
        return ret_buf

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Any, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "Terminate.deserialize(): len(): {} data: {}".format(
                len(data), data.hex(":")
            )
        )
        # 16 bytes
        (sessionId, remaining_bytes) = Uuid.deserialize(ba=data)
        # 1 byte
        (reject_code_int,) = struct.unpack("<B", remaining_bytes[0:1])
        termination_code = TerminationCodeEnum(reject_code_int)
        remaining_bytes = remaining_bytes[1:]
        logging.debug("serialize: padding is {}".format(cls._group_pad))
        remaining_bytes = remaining_bytes[cls._group_pad :]
        # now our group
        (reject_str, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes, group_type=bytes
        )

        new_self = cls(
            session_id=cast(Uuid, sessionId),
            termination_code=termination_code,
            termination_str=cast(bytes, reject_str).decode("latin-1"),
        )
        return (new_self, remaining_bytes)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        serialized = self.serialize()
        return len(serialized)

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("FIXP:\n")
        str_buf.write("  messageType: {}\n".format(str(self._messageType)))
        str_buf.write("  sessionId: {}".format(self._sessionId))
        str_buf.write("  termination_code: {}".format(self._termination_code))
        str_buf.write("  Reason: {}".format(self.termination_str))
        str_buf.write("  blockLength: {}".format(self._block_length))
        str_buf.write("  groupPad: {}".format(self._group_pad))
        return str_buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        logging.debug("Terminate: self={}\nother={}".format(self, value))
        if not isinstance(value, type(self)):
            raise NotImplementedError

        if self.sessionId != value.sessionId:
            logging.debug("{} != {}".format(self.sessionId, value.sessionId))
            return False
        if self._termination_code != value._termination_code:
            logging.debug(
                "{} != {}".format(self._termination_code, value._termination_code)
            )
            return False
        if self._termination_str != value._termination_str:
            logging.debug(
                "{} != {}".format(self._termination_str, value._termination_str)
            )
            return False
        return True
