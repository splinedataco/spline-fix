try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any
from io import StringIO
import struct
import logging
from joshua.fix.messages import SplineFixType
from joshua.fix.data_dictionary import (
    SPLINE_MUNI_DATA_SCHEMA_VERSION,
    SPLINE_FIXP_SESSION_SCHEMA_ID,
)
from joshua.fix.common import (
    align_by,
    FixpMessageType,
)


class Sequence(SplineFixType):
    """Sequence numbering supports ordered delivery and recovery of messages.
    In FIXP, only application messages are sequenced, not session protocol
    messages. A Sequence message (or Context message described below)
    must be used to start a sequenced flow of application messages. Any
    applications message passed after a Sequence message is implicitly
    numbered, where the first message after Sequence has the sequence
    number NextSeqNo.

    Sending a Sequence or Context message on an Unsequenced or None flow
    is a protocol violation.

    Sequence message MUST be used only in a Recoverable or Idempotent flow on a
    non-multiplexed transport.
    """

    _messageType: FixpMessageType = FixpMessageType.Sequence
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    _next_seq_num: int
    # fixed size fields only, no groups
    _block_length: int
    # NextSeqNo u64 8 bytes
    _block_length = 8
    # will want padding of ?(probably 0) before group
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad

    @property
    def next_seq_num(self) -> int:
        return self._next_seq_num

    @next_seq_num.setter
    def next_seq_num(self, value: int) -> None:
        if not isinstance(value, int | float):
            raise NotImplementedError
        nsn = int(value)
        if nsn <= 0:
            raise ValueError(f"Next Sequence number must be positive. Got: {nsn}")
        self._next_seq_num = nsn

    @property
    def template_id(self) -> int:
        return Sequence._messageType.value

    @property
    def schema_id(self) -> int:
        return Sequence._schemaId

    @property
    def version(self) -> int:
        return Sequence._version

    @property
    def blockLength(self) -> int:
        return self._block_length

    # num_groups uses default of 0 in base class

    def __init__(self, next_sequence_number: int) -> None:
        logging.debug(f"Sequence(next_sequence_number={next_sequence_number})")
        # force validation
        self.next_seq_num = next_sequence_number

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("Sequence:\n")
        buf.write("    next_seq_num: {}\n".format(self.next_seq_num))
        return buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        if isinstance(value, int):
            return self.next_seq_num == value
        elif isinstance(value, type(self)):
            return self.next_seq_num == value.next_seq_num
        else:
            raise NotImplementedError

    def __len__(self) -> int:
        return self.block_length

    def serialize(self) -> bytes:
        seq = struct.pack("<Q", self.next_seq_num)
        return seq

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        remaining_bytes = data
        (next_seq,) = struct.unpack("<Q", remaining_bytes[0:8])
        return (cls(next_sequence_number=next_seq), remaining_bytes[8:])

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized"""
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return self._block_length

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    @property
    def block_length(self) -> int:
        return self._block_length


class UnsequencedHeartbeat(SplineFixType):
    """Used when no application level messages have been sent
    during negotiated keep alive time frame.
    UnsequencedHeartbeat is only used by Unsequenced and None
    (one-way-sessions) flows. For Recoverable or Idempotent
    flows a sequence message will be sent as the heartbeat.
    """

    _messageType: FixpMessageType = FixpMessageType.UnsequencedHeartbeat
    _schemaId: int = SPLINE_FIXP_SESSION_SCHEMA_ID
    _version: int = SPLINE_MUNI_DATA_SCHEMA_VERSION
    # fixed size fields only, no groups
    _block_length: int = 0
    # will want padding of ? before group
    _group_pad = align_by(_block_length)
    # we add it to the blockLength so that the BytesData doesn't
    # try to align it again when using struct_size_bytes()
    _block_length += _group_pad

    @property
    def template_id(self) -> int:
        return UnsequencedHeartbeat._messageType.value

    @property
    def schema_id(self) -> int:
        return UnsequencedHeartbeat._schemaId

    @property
    def version(self) -> int:
        return UnsequencedHeartbeat._version

    @property
    def blockLength(self) -> int:
        return self._block_length

    # num_groups uses default of 0 in base class

    def __init__(self) -> None:
        logging.debug("New UnsequencedHeartbeat")

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("UnsequencedHeartbeat: no_data\n")
        return buf.getvalue()

    def __eq__(self, value: Any) -> bool:
        if isinstance(value, type(self)):
            return True
        else:
            raise NotImplementedError

    def __len__(self) -> int:
        return self.block_length

    def serialize(self) -> bytes:
        return bytes()

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """None of data, group_type nor align_to are used"""
        return (cls(), data)

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized"""
        # 32 characters, but each is 1 hex, which is 1/2 an octet
        return self._block_length

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls._block_length

    @property
    def block_length(self) -> int:
        return self._block_length
