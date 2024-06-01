from typing import Any, cast, List, Optional, Sequence, Sized, Tuple, Type, Union
import ctypes
import struct
from dataclasses import dataclass
import logging
from io import StringIO
from joshua.fix.common import align_by
import datetime as dt

try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self


class SplineFixType(Sized):
    """Must implement __len__ in such a way that it
     returns length_in_bytes.
     Because of this, must NOT also inherit from Iterator

     Must also implement block_length, which is the length
     in bytes of the object NOT including any repeating groups
     or variable length fields.

     Example: MuniCurve
     4 bytes rating
     1 byte payment
     1 byte coupon
     1 byte size
     1 byte liquidity
     2 bytes fixed decimal yield * 30

     total length = 68
     block_length = 68 because no groups or variable length items

     Fictional Example: CurveInterval
     8 bytes datetime for YYYY/MM/DD/HH/MM
     CurveGroup
       2 bytes length of curves
       2 bytes num curves

    This gives the number of bytes to read from start to end of message.
     `CurveInterval.length_in_bytes = 8 bytes + 2 bytes + 2 bytes + (curve.total_length * num_curves)`

    This contains only enough data to allow one to read the block data, does not include the group
    To read the group, it's known by the schema that CurveInterval is immediately followed by
    a Group, so after the block is read, then the group structure is read using.
     `CurveInterval.block_length = 8 bytes`
    where
     `Group.block_length = 4`
    This indicates to read 4 bytes, convert into a group of Items defined by the specification,
    where each of the items has it's own block length, and potentially groups.


    To determine total length of a message to put into SOFH, either write everything, including
    SOFH, to a buffer, call len(byte_buffer) and then overwrite the SOFH with the known value, OR,
    call len(message) at the top level which should return the size of all of it's parts recursively
    and must give the same answer as len(message)
    """

    _template_id: int
    _schema_id: int
    _version: int
    _block_length: int

    @property
    def blockLength(self) -> int:
        return self._block_length

    def length_in_bytes(self) -> int:
        """Total length in bytes of object and sub objects recursively."""
        raise NotImplementedError

    def __len__(self) -> int:
        """Total length in bytes of object and sub objects recursively."""
        return self.length_in_bytes()

    @property
    def block_length(self) -> int:
        """Root block length of object being described"""
        raise NotImplementedError

    @property
    def template_id(self) -> int:
        return self._template_id

    @property
    def schema_id(self) -> int:
        return self._schema_id

    @property
    def version(self) -> int:
        return self._version

    @classmethod
    def struct_size_bytes(cls) -> int:
        """Basically root block length of self.
        Sum of the sizes of structures
        """
        raise NotImplementedError

    @classmethod
    def static_block_length(cls, subtype: Type[Self], align_to: int = 4) -> int:
        """Before an object is instantiated, it can't know what type it points
        to. By calling this method it's possible to get the block length
        before instantiating the object. For example, during deserialization.
        """
        raise NotImplementedError

    @property
    def num_groups(self) -> int:
        return 0

    def serialize(self) -> bytes:
        raise NotImplementedError

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any, align_to: int = 4
    ) -> Tuple[Any, bytes]:
        raise NotImplementedError


@dataclass
class GroupSizeEncodingSimple(SplineFixType):
    """Rather than override the default groupSizeEncoding in FIX(a violation of the protocol)
    this is our own that can be used when not all the features in the default are necessary.
    This will support a data lemgth up to 65536, and up to 255 items in each message.
    Because we are intending on keeping the length of each message under 1500 bytes,
    with 68 bytes per curve, that's 4 bytes for the rating, and then enumerations each of one
    byte for payment, coupon, length, and liquidty, for a total of 8 bytes for the featureset
    and then 30 yields each of 2 bytes is 60 bytes. For a total of 68 bytes.
    1500/68 = 22, so with framing and lower protocol overhead, we're likely to have only
    20 curves per message.
    37 curves per interval, so we may only need 2 messages to send all curve data.

    Here is an issue with the specification(s) for SBE. blockLength is defined in all cases
    as "root block length" EXCEPT for groupDimensionType where it is to return the
    size of a "row" in the group,or in other words, the sum of the blockLength of each field
    in a "row".

    So, this means that the block_length and num_in_group sizes don't appear in either
    the parent message blockLength nor in it's own BlockLength. So if you want to know
    how many bytes to read in order to overlay you can't use block length here, so we need
    to, in this one case, know to use NUM_BYTES if we need to know how many bytes to consume
    for the message.
    """

    _block_length: ctypes.c_uint16
    num_in_group: ctypes.c_uint16
    data: Any
    NUM_BYTES = 4
    # we want to be able to fit in a single packet to avoid fragmentation
    # MAX_LEN = 1500 >= 1500- group_header - len(one_object)*num_objects
    # at the time of writing 1500 - 4 - max tcp packet header 60 bytes
    # = 1436
    # with a yield of short, and 10 per curve, a curve is (30*2)+8 bytes 68
    # with an 8 byte date time shared.
    # 1436 - 8 = 1428 / 68 == 21. So we can get about 20-21 curves in a message.
    # with alignment, a curve is 72, so 1428/72 = 19
    # When using to generate an actual message, should get the
    # SOFH len, message header len, and message body and send in
    # as additional_byte_restrict
    MAX_LEN = 1500 - NUM_BYTES - 60
    align_to: int

    @property
    def blockLength(self) -> int:
        return self._block_length.value

    @property
    def num_groups(self) -> int:
        return 1

    def __init__(
        self,
        data: Sequence[SplineFixType],
        additional_byte_restrict: int = 0,
        allow_larger_than_mtu: bool = False,
        align_to: int = 4,
    ):
        """Message can support any types internally, but we're only going
        to support the ones we need.
        Types MUST be homogenous, and all have the same size.
        We will use the len() of the first element to set the group length.
        additional_byte_limit, if provided, will restrict the number of
        available bytes to use by the number passed in.
        Example: If it was known that additional parts of the message to be sent
        contained a datetime, then you may want to pass in 8 so that the overall
        number of bytes available were reduced by 8.
        """
        tmplen = len(data)
        max_elem_len = self.max_elements_allowed(
            single_element=type(data[0]),
            additional_byte_restrict=additional_byte_restrict,
            alignment_by=align_to,
        )
        if max_elem_len < tmplen and allow_larger_than_mtu is False:
            raise ValueError("elements are too large to fit in single MTU")
        if tmplen > 2**16 or tmplen == 0:
            raise ValueError(
                "List len must not be empty or larger than max. Max len: {} got len: {}".format(
                    2**16, tmplen
                )
            )
        self.num_in_group = ctypes.c_uint16(len(data))
        logging.debug("self.numInGroup set to: {}".format(self.num_in_group))
        # used in __str__
        self.align_to = align_to
        self.alignment_padding = align_by(len(data[0]), align_to)
        logging.debug(
            "align_to={} self.alignment_padding = {}".format(
                align_to, self.alignment_padding
            )
        )
        # NOTE: block_length now includes the alignment_padding
        self._block_length = ctypes.c_uint16(len(data[0]) + self.alignment_padding)
        # here, based on the spec, this will only contain the size of 1 element
        # plus padding.
        # size of one element plus padding multiplied
        # to get total length
        logging.debug(
            "length_of_one/self._block_length={}".format(self._block_length.value)
        )
        # if we need to be byte aligned, we also need to appropriately align our group leader
        # designed to be aligned on 4 bytes, if requested alignment is > 4 then we need to pad
        self.leader_padding = align_by(self.NUM_BYTES, align_to)
        self.block_length_padding = align_by(self.block_length, align_to)
        # start with mtu. subtract any extra we've been asked to remove to account for other things
        # then, make sure there is room for our element size and number of elements
        # and then any padding that is needed
        # what is left is what may be used to put elements
        self.available_bytes = (
            self.MAX_LEN - additional_byte_restrict - self.leader_padding
        )

        # if block_length > self.available_bytes this will need to be broken into multiple
        # messages
        if (
            self._block_length.value * self.num_in_group.value
        ) > self.available_bytes and not allow_larger_than_mtu:
            raise ValueError(
                "elements are too large to fit in single MTU. available_bytes={} single_element={} num_elements={}".format(
                    self.available_bytes,
                    self._block_length.value,
                    self.num_in_group.value,
                )
            )
        self.data = data

    @classmethod
    def max_elements_allowed(
        cls,
        single_element: Type[SplineFixType],
        additional_byte_restrict: int = 0,
        alignment_by=4,
    ) -> int:
        """Given a single element that must provide number of bytes needed
        when len(element) is called, and an optional additiona_byte_restrict,
        return to caller the maximum number of elements that may be fit into
        a single packet.
        Returns a count of the number of elements that can fit in a packet
        """
        if alignment_by is None or alignment_by == 0:
            alignment_by = 1
        elem_size = single_element.struct_size_bytes()
        group_leader_offset = align_by(cls.NUM_BYTES, alignment_by)
        # start with MTU, minus max TCP headers, subtract our sizeof(blockLength),
        # and sizeof(num_elements), then subtract any additional padding required
        # what's left is able to be divided amongst the elements
        max_avail_bytes = cls.MAX_LEN - group_leader_offset - additional_byte_restrict
        aligned_elem_size = elem_size + align_by(elem_size, alignment_by)
        logging.debug(
            "max_elements_allowed: max_avail_bytes: {} elem_sz({}) extra_b({}) align_by({}) aligned_element_size={}".format(
                max_avail_bytes,
                elem_size,
                additional_byte_restrict,
                alignment_by,
                aligned_elem_size,
            )
        )
        return max_avail_bytes // aligned_elem_size

    def serialize(self) -> bytes:
        """Given a list of data, we will call len() on one of the objects and assume
        a uniform size for all the objects in the list. Based on that we will limit
        to below a certain size (just so we can fit into a MTU) and update the length
        and size fields as appropriate.
        """
        # structure for GroupSizeEncodingSimple is:
        #
        # send the length of one "row" or element (may be multiple fields)
        ser_ret = struct.pack("<H", self._block_length.value)
        # then we send the count of the number of elements
        # the type is inferred from the outside schema(outside this class)
        ser_ret += struct.pack("<H", self.num_in_group.value)
        logging.debug("if leader padding is needed, put it in now")
        if self.leader_padding > 0:
            ser_ret += struct.pack(
                f"<{self.leader_padding}H",
                bytes([0 for _ in range(0, self.leader_padding, 1)]),
            )
        logging.debug(
            "group serialize: type(self.data[0]): {!r} data: {!r} leader: {!r}".format(
                type(self.data[0]), self.data, ser_ret
            )
        )

        # now the content
        for item in self.data:
            ser_ret += item.serialize()
            if self.block_length_padding > 0:
                ser_ret += bytes([0 for _ in range(0, self.block_length_padding, 1)])

        logging.debug("serialized group as: {!r}".format(ser_ret))
        return ser_ret

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any, align_to: int = 4
    ) -> Tuple[Sequence[Any], bytes]:
        """Should make a Serialize interface: group_type object must support serialize.
        Takes the expected object type in the data stream and returns a list of
        0 to n objects of same type as a list.

        Data stream passed in must start at the beginning of the group section.
        Group will read 6 bytes, deserialize to a size and count, read that number of
        bytes minus size of self. Then will start deserializing using the object.
        This continues until the read data size is 0.
        """
        leader_padding = align_by(
            numbytes=cls.struct_size_bytes(), alignment_by=align_to
        )
        logging.debug("group.deser: leader_padding={}".format(leader_padding))
        # read our group leader and any padding.
        read_start = 0
        read_end = 2
        (block_length,) = struct.unpack("<H", data[read_start:read_end])
        logging.debug(
            "group.deser: block_length(size of 1 row)={}".format(block_length)
        )
        read_start = read_end
        # 2 bytes for a short plus padding bytes.
        read_end = read_start + 2 + leader_padding
        logging.debug(
            "group.deser read_end before group_len with read_start = {} and padding of {}".format(
                read_start, leader_padding
            )
        )
        (num_elem_in_group,) = struct.unpack(
            f"<H{leader_padding}x", data[read_start:read_end]
        )
        read_start = read_end
        logging.debug(
            "deserialized group leader and padding: read_start now: {} group_size: {} number_of_elements: {}".format(
                read_start, block_length, num_elem_in_group
            )
        )
        elem_list: List[Any] = []
        # create a slice from the data handed us that consists only of the data for our group
        # the length we get is inclusive of the group leaders, so we subtract it from the length
        # we've already advanced the read_start, and incorporated the leader_padding, if any
        len_all_elements = block_length * num_elem_in_group
        remaining_data = data[read_start : read_start + len_all_elements]
        row_padding: Optional[int] = None
        while len(remaining_data) > 0 and num_elem_in_group - len(elem_list) > 0:
            logging.debug(
                "group.deser: loop enter: len(remaining_data)={} remaining_items={}".format(
                    len(remaining_data), num_elem_in_group - len(elem_list)
                )
            )
            # element must know how to deserialize itself and return info
            (element, remaining_data) = group_type.deserialize(data=remaining_data)
            """# commented out because profiling showd a lot of time being spent here
            logging.debug(
                "got element: remaining bytes:{} element:{}".format(
                    len(remaining_data), element
                )
            )
            """
            # the element doesn't know about external padding, and we didn't know it's
            # size until it was created.
            if row_padding is None:
                row_padding = align_by(numbytes=len(element), alignment_by=align_to)
            # just skip any padding
            remaining_data = remaining_data[row_padding:]
            elem_list.append(element)
        if len(remaining_data) != 0:
            raise IOError(
                "Serializing group completed with data remaining in buffer. Expected number of elements: {} read number of elements: {}".format(
                    num_elem_in_group, len(elem_list)
                )
            )
        if len(elem_list) - num_elem_in_group != 0:
            raise IOError(
                "Serializing group and received fewer elements than expected. len(remaining_buffer)= {} expected_num_elements: {} number_elem_read={}".format(
                    len(remaining_data), num_elem_in_group, len(elem_list)
                )
            )
        return (elem_list, data[read_start + len_all_elements :])

    def __str__(self) -> str:
        outbuf = StringIO()
        outbuf.write("GroupSizeEncodingSimple:\n")
        outbuf.write(
            "  static_block_length={}\n".format(
                self.static_block_length(subtype=self.data[0], align_to=self.align_to)
            )
        )
        outbuf.write("  self.block_length={}\n".format(self.block_length))
        outbuf.write("  self._block_length={}".format(self._block_length))
        outbuf.write("  numInGroup={}\n".format(self.num_in_group.value))
        outbuf.write("  data={}\n".format(self.data))
        outbuf.write(
            "  NUM_BYTES(of self group info)={}\n".format(self.struct_size_bytes())
        )
        outbuf.write("  MAX_LEN(mtu - stuff)={}\n".format(self.MAX_LEN))
        outbuf.write("  leader_padding={}\n".format(self.leader_padding))
        outbuf.write("  length_in_bytes={}".format(self.length_in_bytes()))

        return outbuf.getvalue()

    def length_in_bytes(self) -> int:
        # block_length is the length of one element.
        # therefore, times the number in the group
        #
        return (
            self._block_length.value * self.num_in_group.value
            + self.struct_size_bytes()
            + self.leader_padding
        )

    # NOTE: intending to leave block_length(cls) from SplineFixType
    #       as unimplemented due to the fact that based on the FIXsbe spec
    #       defines/uses the blockLength differently for groupdimensions
    #       than it does for other things.
    #
    #       Attempting to override with a new function that takes a subtype
    @classmethod
    def static_block_length(
        cls, subtype: Type[SplineFixType], align_to: int = 4
    ) -> int:
        # by spec, it is the length of a "row" in a group.
        elem_len = subtype.struct_size_bytes()
        per_elem_pad = align_by(elem_len, align_to)
        return elem_len + per_elem_pad

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES

    @property
    def block_length(self) -> int:
        return self._block_length.value


class SplineDateTime:
    """Store as int to avoid float conversion issues. u32. Keeps second resolution."""

    _timestamp: int
    NUM_BYTES = 4

    @property
    def blockLength(self) -> int:
        return self.NUM_BYTES

    def __init__(self, other: Optional[Union[dt.datetime, float]] = None):
        self.timestamp = other

    def to_datetime(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self._timestamp)

    def __str__(self) -> str:
        str_buf = StringIO()
        str_buf.write("SplineDateTime: ")
        str_buf.write(f"{self.to_datetime()}")
        return str(str_buf.getvalue())

    def serialize(self) -> bytes:
        # not using floating part
        ts = int(self._timestamp)
        b = struct.pack("<L", ts)
        logging.debug("SplineDateTime.serialize turn {} to {!r}".format(ts, b.hex(":")))
        return b

    @classmethod
    def deserialize(cls, byte_stream: bytes) -> Tuple[Self, bytes]:
        if len(byte_stream) < cls.NUM_BYTES:
            raise ValueError(
                "SplineDateTime needs at least {} bytes to deserialize. Got: {}".format(
                    cls.NUM_BYTES, str(len(byte_stream))
                )
            )
        (ts,) = struct.unpack("<L", byte_stream[: cls.NUM_BYTES])
        return (cls(float(ts)), byte_stream[cls.NUM_BYTES :])

    def __eq__(self, object) -> bool:
        return self._timestamp == object._timestamp

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES

    @property
    def datetime(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.timestamp)

    @datetime.setter
    def datetime(self, dt_tm: dt.datetime) -> None:
        if not isinstance(dt_tm, dt.datetime):
            raise NotImplementedError
        self._timestamp = int(dt_tm.timestamp())

    @property
    def timestamp(self) -> float:
        return float(self._timestamp)

    @timestamp.setter
    def timestamp(self, value: Optional[Union[dt.datetime, float, Self]]) -> None:
        if isinstance(value, dt.datetime):
            """this would be more efficient to write by having the
            `if other is None: other = datetime.now()`
            part first and then convert to timestamp, but since
            this is the fast path, doing it this way for speed
            """
            self._timestamp = int(value.timestamp())
            return
        elif value is None:
            value = dt.datetime.now()
            self._timestamp = int(value.timestamp())
            return
        if isinstance(value, int | float):
            self._timestamp = int(value)
            return
        logging.error(f"SplineDateTime(type(other)={type(value)})")
        raise NotImplementedError


class SplineNanoTime(SplineDateTime):
    """Time is stored in nanoseconds integer (u64) to avoid
    resolution problems with floats
    """

    _timestamp: int = 0
    __fmt_str = "<Q"
    # 8 bytes u64
    NUM_BYTES = struct.calcsize(__fmt_str)
    NANOSECONDS = 1_000_000_000

    def __init__(self, other: Optional[Union[dt.datetime, float, Self]] = None):
        self.timestamp = other
        logging.debug(
            f"SplineNanoTime.init ending with self._timestamp = {self._timestamp}"
        )

    def serialize(self) -> bytes:
        b = struct.pack(self.__fmt_str, self._timestamp)
        logging.debug(
            "SplineNanoTime.serialize turn {} to {!r}".format(
                self._timestamp, b.hex(":")
            )
        )
        return b

    @classmethod
    def deserialize(cls, byte_stream: bytes) -> Tuple[Self, bytes]:
        if len(byte_stream) < cls.NUM_BYTES:
            raise ValueError(
                "SplineNanoTime needs at least {} bytes to deserialize. Got: {}".format(
                    cls.NUM_BYTES, str(len(byte_stream))
                )
            )
        (ts,) = struct.unpack(cls.__fmt_str, byte_stream[: cls.NUM_BYTES])
        ts = cast(int, ts)

        return (cls(ts), byte_stream[cls.NUM_BYTES :])

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, type(self)):
            raise NotImplementedError
        return self.timestamp == value.timestamp

    def as_timestamp(self) -> float:
        return float(self._timestamp / self.NANOSECONDS)

    def to_datetime(self) -> dt.datetime:
        return dt.datetime.fromtimestamp(self.as_timestamp())

    @property
    def datetime(self) -> dt.datetime:
        """In order to override the setter, the getter
        must also be defined, even though the base is
        being called.
        """
        return super(SplineDateTime, self).datetime

    @datetime.setter
    def datetime(self, dt_tm: dt.datetime) -> None:
        if not isinstance(dt_tm, dt.datetime):
            raise NotImplementedError
        self.timestamp = dt_tm.timestamp()

    @property
    def timestamp(self) -> float:
        return self.as_timestamp()

    @timestamp.setter
    def timestamp(self, value: Optional[Union[dt.datetime, float, Self]]) -> None:
        logging.debug(f"SplineNanoTime.timestamp = {value}")
        if value is None:
            logging.debug("value is none, setting to now")
            value = cast(dt.datetime, dt.datetime.now())
        if isinstance(value, dt.datetime):
            logging.debug(
                "value is a datetime converting to datetime.timestamp() (a float)"
            )
            value = cast(float, value.timestamp())
        if isinstance(value, type(self)):
            logging.debug(
                "one of us! one of us! setting self._timestamp = value._timestamp"
            )
            self._timestamp = value._timestamp
        elif isinstance(value, float):
            logging.debug(
                "value is a float multiplying by 1MM to convert to nanoseconds"
            )
            # treat as a python timestamp
            self._timestamp = int(value * self.NANOSECONDS)
        elif isinstance(value, int):
            logging.debug(
                "value is an int, assuming already in nanoseconds and assigning to self._timestamp"
            )
            # treat as u64 ns timestamp
            self._timestamp = value
        else:
            raise NotImplementedError
        logging.debug(
            f"exiting setter.timestamp with self._timestamp = {self._timestamp}"
        )


class SplineByte(SplineFixType):
    """Just a wrapper for a single byte (int)
    to limit it's size, scope, and to return proper
    sizes so can be used in a group.
    """

    _value: int

    def __init__(self, value: int):
        if not isinstance(value, int):
            raise NotImplementedError
        if value < 0 or value > 255:
            raise ValueError("a byte must be in the range [0:255]")

        self._value = value

    def length_in_bytes(self) -> int:
        """Total length in bytes of object and sub objects recursively."""
        return 1

    @property
    def block_length(self) -> int:
        """Root block length of object being described"""
        return 1

    @classmethod
    def struct_size_bytes(cls) -> int:
        """Basically root block length of self.
        Sum of the sizes of structures
        """
        return 1

    @classmethod
    def static_block_length(cls, subtype: Type[Self], align_to: int = 4) -> int:
        """Before an object is instantiated, it can't know what type it points
        to. By calling this method it's possible to get the block length
        before instantiating the object. For example, during deserialization.
        """
        return 1

    def serialize(self) -> bytes:
        return struct.pack("<B", self._value)

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any, align_to: int = 4
    ) -> Tuple[Self, bytes]:
        raise NotImplementedError
        (value,) = struct.unpack("<B", data[0:1])
        return (cast(Any, cls(value)), data[1:])


class SplineByteData(GroupSizeEncodingSimple):
    # can just use the same init
    data: bytes

    @property
    def num_groups(self) -> int:
        return 1

    def __init__(
        self,
        # actually a Sequence[int], but we can't differentiate
        data: bytes,
        additional_byte_restrict: int = 0,
        allow_larger_than_mtu: bool = False,
        align_to: int = 4,
    ):
        """
        wrapped_data = [SplineByte(b) for b in data]
        GroupSizeEncodingSimple.__init__(
            self,
            wrapped_data,
            additional_byte_restrict,
            allow_larger_than_mtu,
            align_to,
        )
        """
        tmplen = len(data)
        if tmplen > 0:
            max_elem_len = self.max_elements_allowed(
                single_element=type(data),
                additional_byte_restrict=additional_byte_restrict,
                alignment_by=align_to,
            )
        else:
            max_elem_len = 0
        if max_elem_len < tmplen and allow_larger_than_mtu is False:
            raise ValueError("elements are too large to fit in single MTU")
        # we're going to allow == 0 so we can treat that as Optional
        if tmplen > 2**16:
            raise ValueError(
                "List len must not be empty or larger than max. Max len: {} got len: {}".format(
                    2**16, tmplen
                )
            )
        self.num_in_group = ctypes.c_uint16(len(data))
        logging.debug("self.numInGroup set to: {}".format(self.num_in_group))
        # used in __str__
        self.align_to = align_to
        self.alignment_padding = 0
        logging.debug(
            "align_to={} self.alignment_padding = {}".format(
                align_to, self.alignment_padding
            )
        )
        # NOTE: block_length now includes the alignment_padding
        # 1 if we have bytes to send, 0 if we don't
        self._block_length = ctypes.c_uint16(int(tmplen > 0))
        # here, based on the spec, this will only contain the size of 1 element
        # plus padding.
        # size of one element plus padding multiplied
        # to get total length
        logging.debug(
            "length_of_one/self._block_length={}".format(self._block_length.value)
        )
        # if we need to be byte aligned, we also need to appropriately align our group leader
        # designed to be aligned on 4 bytes, if requested alignment is > 4 then we need to pad
        self.leader_padding = align_by(self.NUM_BYTES, align_to)
        self.block_length_padding = 0
        # start with mtu. subtract any extra we've been asked to remove to account for other things
        # then, make sure there is room for our element size and number of elements
        # and then any padding that is needed
        # what is left is what may be used to put elements
        self.available_bytes = (
            self.MAX_LEN - additional_byte_restrict - self.leader_padding
        )

        # if block_length > self.available_bytes this will need to be broken into multiple
        # messages
        if (
            self._block_length.value * self.num_in_group.value
        ) > self.available_bytes and not allow_larger_than_mtu:
            raise ValueError(
                "elements are too large to fit in single MTU. available_bytes={} single_element={} num_elements={}".format(
                    self.available_bytes,
                    self._block_length.value,
                    self.num_in_group.value,
                )
            )
        self.data = data

    @classmethod
    def max_elements_allowed(
        cls,
        single_element: Type[bytes],
        additional_byte_restrict: int = 0,
        alignment_by=1,
    ) -> int:
        """Given a single element that must provide number of bytes needed
        when len(element) is called, and an optional additiona_byte_restrict,
        return to caller the maximum number of elements that may be fit into
        a single packet.
        Returns a count of the number of elements that can fit in a packet
        """
        if single_element != bytes:
            raise ValueError(
                "SplineByteData requires only bytes as single_element type"
            )
        if alignment_by is None or alignment_by == 0:
            alignment_by = 1
        # size is one, but we don't want to align after every one, just the first
        elem_size = 1
        group_leader_offset = align_by(cls.NUM_BYTES, alignment_by)
        # start with MTU, minus max TCP headers, subtract our sizeof(blockLength),
        # and sizeof(num_elements), then subtract any additional padding required
        # what's left is able to be divided amongst the elements
        max_avail_bytes = cls.MAX_LEN - group_leader_offset - additional_byte_restrict
        aligned_elem_size = elem_size
        logging.debug(
            "max_elements_allowed: max_avail_bytes: {} elem_sz({}) extra_b({}) align_by({}) aligned_element_size={}".format(
                max_avail_bytes,
                elem_size,
                additional_byte_restrict,
                alignment_by,
                aligned_elem_size,
            )
        )
        return max_avail_bytes // aligned_elem_size

    def serialize(self) -> bytes:
        """Given a list of data, we will call len() on one of the objects and assume
        a uniform size for all the objects in the list. Based on that we will limit
        to below a certain size (just so we can fit into a MTU) and update the length
        and size fields as appropriate.

        Since it's just bytes, we're little endian, so we can just return our data.
        Well, and the group info and leader padding.
        """
        # structure for GroupSizeEncodingSimple is:
        #
        # send the length of one "row" or element (may be multiple fields)
        ser_ret = struct.pack("<H", self._block_length.value)
        # then we send the count of the number of elements
        # the type is inferred from the outside schema(outside this class)
        ser_ret += struct.pack("<H", self.num_in_group.value)
        logging.debug("if leader padding is needed, put it in now")
        if self.leader_padding > 0:
            ser_ret += struct.pack(
                f"<{self.leader_padding}H",
                bytes([0 for _ in range(0, self.leader_padding, 1)]),
            )
        logging.debug(
            "group serialize: type(self.data): {!r} data: {!r} leader: {!r}".format(
                type(self.data), self.data, ser_ret
            )
        )

        # now the content
        ser_ret += self.data
        logging.debug("serialized group as: {!r}".format(ser_ret))
        return ser_ret

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any, align_to: int = 4
    ) -> Tuple[Sequence[Any], bytes]:
        """Note: group_type not used
        Should make a Serialize interface: group_type object must support serialize.
        Takes the expected object type in the data stream and returns a list of
        0 to n objects of same type as a list.

        Data stream passed in must start at the beginning of the group section.
        Group will read 6 bytes, deserialize to a size and count, read that number of
        bytes minus size of self. Then will start deserializing using the object.
        This continues until the read data size is 0.
        """
        leader_padding = align_by(
            numbytes=cls.struct_size_bytes(), alignment_by=align_to
        )
        logging.debug("group.deser: leader_padding={}".format(leader_padding))
        # read our group leader and any padding.
        read_start = 0
        read_end = 2
        (block_length,) = struct.unpack("<H", data[read_start:read_end])
        logging.debug(
            "group.deser: block_length(size of 1 row)={}".format(block_length)
        )
        read_start = read_end
        # 2 bytes for a short plus padding bytes.
        read_end = read_start + 2 + leader_padding
        logging.debug(
            "group.deser read_end before group_len with read_start = {} and padding of {}".format(
                read_start, leader_padding
            )
        )
        (num_elem_in_group,) = struct.unpack(
            f"<H{leader_padding}x", data[read_start:read_end]
        )
        read_start = read_end
        logging.debug(
            "deserialized group leader and padding: read_start now: {} group_size: {} number_of_elements: {}".format(
                read_start, block_length, num_elem_in_group
            )
        )
        if num_elem_in_group == 0:
            return (bytes(), data[read_end:])

        return (
            data[read_start : read_start + num_elem_in_group],
            data[read_start + num_elem_in_group :],
        )

    def __str__(self) -> str:
        outbuf = StringIO()
        outbuf.write("GroupSizeEncodingSimple:\n")
        outbuf.write("  static_block_length={}\n".format(1))
        outbuf.write("  self.block_length={}\n".format(self.block_length))
        outbuf.write("  self._block_length={}".format(self._block_length))
        outbuf.write("  numInGroup={}\n".format(self.num_in_group.value))
        outbuf.write("  data={!r}\n".format(self.data))
        outbuf.write(
            "  NUM_BYTES(of self group info)={}\n".format(self.struct_size_bytes())
        )
        outbuf.write("  MAX_LEN(mtu - stuff)={}\n".format(self.MAX_LEN))
        outbuf.write("  leader_padding={}\n".format(self.leader_padding))
        outbuf.write("  length_in_bytes={}".format(self.length_in_bytes()))

        return outbuf.getvalue()

    def __eq__(self, value: Any) -> bool:
        if not isinstance(value, type(self)):
            raise NotImplementedError
        if self.data != value.data:
            logging.debug(
                "self.data != value.data\n{!r} != {!r}".format(self.data, value.data)
            )
            return False
        return True
