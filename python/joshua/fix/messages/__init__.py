r"""
Relevant documentation
Simple Open Framing Header
https://www.fixtrading.org/standards/fix-sofh-online/
Simple Binary Encoding
https://www.fixtrading.org/standards/sbe-online/

Needed for streams (like TCP) not needed for message oriented feeds (UDP, SCTP)

NOTE: SOFH is representd in network byte order '!' so, the
encoding for sbe_little_endian can be read from the network, in network byte order,
and if they are in '\xEB\x50' order, then it's little endian.
If it shows up as '\x5B\xE0', then it is in big endian.
2 fields:
Message_Length 4 octets
Encoding_Type  4 octets

hex_string = "0xEB50"
asint = int(hex_string, 16)
hex_val = hex(as_int)

MessageSchema attributes:
package - Name or category of a schema : string | optional | unique between counterparties
 example: "SplineMunis"
id - unique identifier of a schema : unsignedInt |  | unique between counterparties
   example: 42
version - version of this schema : nonnegativeInteger| | Initial version is 0, increment by 1 for each version
   example 0
semanticVersion - version of DIX semantics | optional | FIX versions such as "FIX.5.0_SP2"
byteOrder - Byte order of encoding : token | default =| littleEndian or bigEndian
headerType -  Name of the encoding type of the message header,
              which is the same for ALL messages in a schema| string |
              default=messageHeader| encoding with this name must be contained by <types>

templateId - message id. What message from this schema and version to use to interpret the following message
"""

from enum import Enum
from typing import Tuple
import ctypes
import struct
from io import StringIO
import logging
from joshua.fix.types import GroupSizeEncodingSimple, SplineFixType
from joshua.fix.common import SplineMuniFixType, FixpMessageType
from joshua.fix import data_dictionary

try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any

MuniPayment = Enum("MuniPayment", ["go", "rev"])
MuniSize = Enum("MuniSize", ["sml", "med", "lrg"])
MuniLiquidity = Enum("MuniLiquidity", ["ilqd", "mlqd", "lqd"])


class SplineMessageHeader(SplineFixType):
    """Following from https://www.fixtrading.org/standards/sbe-online section
    "Message header schema"
    Recommended message header encoding:
    ```xml
    <composite name="messageHeader" description="Template ID and length of message root">
        <type name="blockLength" primitiveType="uint16"/>
        <type name="templateId" primitiveType="uint16"/>
        <type name="schemaId" primitiveType="uint16"/>
        <type name="version" primitiveType="uint16"/>
        <type name="numGroups" primitiveType="uint16" />
        <type name="numVarDataFields" primitiveType="uint16" />
    </composite>
    ```

    Where:
    blockLength is the root block length of the message that follows. Used for when codex
    doesn't understand the message.
    templateId = tag=35 equivalent, messageId=we decide numeric. Unique per schema.
    version = incremented each schema change. Used to choose the proper template.
              Essentially makes the lookup (in pseudocode):
              `message_struct = messages[schemaId][templateId][version]`
    numGroups - represents the number of groups contained in the message that follows.
                Can be used when message is not understood by codex. Ex:
                ```pseudo
                if don't understand message then:
                    read/consume/sink blockLength bytes
                    // this brings us to the group
                    use the defined group structure
                    read/consume/sink group.blockLength*group.numInGroup
                ```

                Really only useful if there is an extension to the message and
                additional groups are added; because the types/fields in the
                group are only defined in the specification, and if the codex
                doesn't support the version then it won't know how to interpret
                the group and needs to be able to skip it.

                If the codex doesn't support the message at all, then it should
                use the SOFH length to skip the full length of the message
                leaving the current read pointer at the next SOFH.
    numVardataFields - we don't need this if we never plan on using variable length
                       fields in our schemas. Only scenarios I can think of are
                       if we wanted to send raw data, like a file, over fix.
                       And then we could make our own message that contained
                       a full length in bytes, and send as many bytes as will
                       fit in a message.

    Total length = 12 + SOFH 6 = 18, which is not aligned.
    We won't use variadic fields, so we can drop the numVarDataFields which brings us to 16 bytes.

    blockLength is "root block length", so is probably the blockLength of the "MuniCurvesMessage"
    Which would be 8, just for the datetime.

    So, suggesting for spline:
    ```xml
    <composite name="messageHeader" description="same">
        <type name="blockLength" primitiveType="uint16"/>
        <type name="templateId" primitiveType="uint8"/>
        <type name="schemaId" primitiveType="uint8"/>
        <type name="version" primitiveType="uint8"/>
        <type name="numGroups" primitiveType="uint8"/>
    ```

    This permits a root block length of a message to be up to 2^16 bytes,
    far larger than we should need, but 2^8 is probably too small.
    For templateId, we don't anticipate needing more than 255 messages per schema.
    For schemaId, we shouldn't have more than 255 different schemas.
    For version, if we hit 256 versions, we can make a new schema.
    """

    _block_length: ctypes.c_uint16
    _template_id: ctypes.c_uint8
    _schema_id: ctypes.c_uint8
    _version: ctypes.c_uint8
    _num_groups: ctypes.c_uint8
    NUM_BYTES: int = 6

    def __init__(
        self,
        block_length: int,
        template_id: int,
        schema_id: int,
        version: int,
        num_groups: int,
    ):
        if block_length > 2**16:
            raise ValueError(
                "block_length is too large to fit in 2**16. Got: {}".format(
                    block_length
                )
            )
        elif block_length < 0:
            raise ValueError(
                f"block_length must be a non-negative integer. Got: {block_length}"
            )
        self._block_length = ctypes.c_uint16(block_length)

        param_vals = [template_id, schema_id, version, num_groups]
        param_names = ["template_id", "schema_id", "version", "num_groups"]
        max_byte = 2**8
        for v, n in zip(param_vals, param_names):
            if v > max_byte or v < 0:
                raise ValueError(
                    f"{n} must be a non-negative number less than {max_byte}. Got: {v}"
                )
            setattr(self, n, ctypes.c_uint8(v))

    @property
    def template_id(self) -> int:
        return self._template_id.value

    @template_id.setter
    def template_id(self, value: int) -> None:
        if isinstance(value, ctypes.c_ubyte):
            self._template_id = ctypes.c_uint8(value.value)
        elif value > 2**8 or value < 0:
            raise ValueError(
                f"template_id must be a non-negative number less than {2**8}. Got: {value}"
            )
        else:
            self._template_id = ctypes.c_uint8(value)

    @property
    def schema_id(self) -> int:
        return self._schema_id.value

    @schema_id.setter
    def schema_id(self, value: int) -> None:
        if isinstance(value, ctypes.c_ubyte):
            self._schema_id = ctypes.c_uint8(value.value)
        elif value > 2**8 or value < 0:
            raise ValueError(
                f"schema_id must be a non-negative number less than {2**8}. Got: {value}"
            )
        else:
            self._schema_id = ctypes.c_uint8(value)

    @property
    def version(self) -> int:
        return self._version.value

    @version.setter
    def version(self, value: int) -> None:
        if isinstance(value, ctypes.c_ubyte):
            self._version = ctypes.c_uint8(value.value)
        elif value > 2**8 or value < 0:
            raise ValueError(
                f"version must be a non-negative number less than {2**8}. Got: {value}"
            )
        else:
            self._version = ctypes.c_uint8(value)

    @property
    def num_groups(self) -> int:
        return self._num_groups.value

    @num_groups.setter
    def num_groups(self, value: int) -> None:
        if isinstance(value, ctypes.c_ubyte):
            self._num_groups = ctypes.c_uint8(value.value)
        elif value > 2**8 or value < 0:
            raise ValueError(
                f"num_groups must be a non-negative number less than {2**8}. Got: {value}"
            )
        else:
            self._num_groups = ctypes.c_uint8(value)

    @property
    def block_length(self) -> int:
        return self._block_length.value

    def length_in_bytes(self) -> int:
        """Return length of self, and to the end of contained data.
        When deserializing, we would have to look up the message,
        The group, etc. When building, and serializing the information
        may be available. So here we're returning only what we absolutely
        know must follow.
        """
        # size of ourself
        tot_len = self.struct_size_bytes()
        # we know the size of the blockLength that follows
        tot_len += self.block_length
        # we know if there is a group, and that each group must have our group header
        tot_len += self.num_groups * GroupSizeEncodingSimple.struct_size_bytes()
        return tot_len

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES

    def serialize(self) -> bytes:
        byte_buf = bytes()
        byte_buf += struct.pack("<H", self.block_length)
        byte_buf += struct.pack("<B", self.template_id)
        byte_buf += struct.pack("<B", self.schema_id)
        byte_buf += struct.pack("<B", self.version)
        byte_buf += struct.pack("<B", self.num_groups)
        return byte_buf

    @classmethod
    def deserialize(
        cls, byte_buf: bytes, group_type: Any = None, align_to: int = 4
    ) -> Tuple[Self, bytes]:
        """group_type and align_to are unused"""
        if len(byte_buf) < cls.struct_size_bytes():
            raise ValueError(
                "MessageHeader requires at least {} bytes. Provided buffer len: {}".format(
                    cls.struct_size_bytes(), len(byte_buf)
                )
            )
        (block_len,) = struct.unpack("<H", byte_buf[0:2])
        (template_id,) = struct.unpack("<B", byte_buf[2:3])
        (schema_id,) = struct.unpack("<B", byte_buf[3:4])
        (version,) = struct.unpack("<B", byte_buf[4:5])
        (num_grps,) = struct.unpack("<B", byte_buf[5:6])
        return (cls(block_len, template_id, schema_id, version, num_grps), byte_buf[6:])

    def __str__(self) -> str:
        type_name: str
        match (self.template_id, self.schema_id):
            case data_dictionary.SPLINE_FIXP_SESSION_SCHEMA_ID:
                type_name = FixpMessageType(self.template_id).name
            case data_dictionary.SPLINE_MUNI_DATA_SCHEMA_ID:
                type_name = SplineMuniFixType.Curve.name
            case _:
                type_name = "Unknown"
        buf = StringIO()
        buf.write("SplineMessageHeader\n")
        buf.write(f"   block_length: {self.block_length}\n")
        buf.write(f"   template_id : {self.template_id} : {type_name}\n")
        buf.write(f"   schema_id   : {self.schema_id}\n")
        buf.write(f"   version     : {self.version}\n")
        buf.write(f"   num_groups  : {self.num_groups}\n")
        return buf.getvalue()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SplineMessageHeader):
            return NotImplemented
        my_tuple = (
            self.block_length,
            self.template_id,
            self.schema_id,
            self.version,
            self.num_groups,
            self.NUM_BYTES,
        )
        other_tuple = (
            other.block_length,
            other.template_id,
            other.schema_id,
            other.version,
            other.num_groups,
            other.NUM_BYTES,
        )
        logging.debug(
            "SplineMessageHeader.eq(): {} == {}".format(my_tuple, other_tuple)
        )
        return my_tuple == other_tuple
