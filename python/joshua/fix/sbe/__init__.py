try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from dataclasses import dataclass
import struct
from typing import Tuple
import logging

"""
Relevant documentation
Simple Open Framing Header
https://www.fixtrading.org/standards/fix-sofh-online/
Simple Binary Encoding
https://www.fixtrading.org/standards/sbe-online/

Needed for streams (like TCP) not needed for message oriented feeds (UDP, SCTP)

NOTE: SOFH is representd in network byte order '!' so, the
encoding for sbe_little_endian can be read from the network, in network byte order,
and if they are in '\xeb\x50' order, then it's little endian.
If it shows up as '\x5b\xe0', then it is in big endian.
2 fields:
Message_Length 4 octets
Encoding_Type  2 octets

hex_string = "0xEB50"
asint = int(hex_string, 16)
hex_val = hex(as_int)
"""

# FIX SBE Version 1.0 Little Endian
sbe_little_endian = b"\xeb\x50"


def fits_in_bytes(num_bytes: int, value: int) -> bool:
    logging.debug("fits_in_bytes(): is {} <= {}".format(value, (2 ** (num_bytes * 8))))
    return value <= (2 ** (num_bytes * 8))


@dataclass
class SbeMessageEncodingHeader:
    """to tell which message template was used to encode the message
    and to give information about the size of the message body to aid in decoding,
    even when a message template has been extended in a later version.

    Fields of SBE header are:
    * Block length of the message root - total space reserved for the root level of the message
                                       not counting any repeating groups or variable length fields
    * Template ID - identifier of th message template. Equivalent to tag=35
    * Schema ID - identifier of the message schema that contains the template
    * Schema version - the version of the message schema in which the message is defined
    * Group count - the number of repeating groups in the root level of the message
    * Variable-length field count - the number of variable-length fields in the root level of the message

    Block length is specified in the message schema but also serialized on the wire.
    By default, block length is set to the sum of the sizes of the body fields in the message.
    """

    # uint16
    # offset=0, octets=2
    block_length: int
    # uint16
    # offset=2, octets=2
    template_id: int
    # uint16
    # offset=4, octets=2
    schema_id: int
    # uint16
    # offset=6, octets=2
    version: int
    # uint16
    # offset=8, octets=2
    num_groups: int
    # uint16
    # offset=10, octets=2
    num_var_data_fields: int

    order: Tuple = (
        "block_length",
        "template_id",
        "schema_id",
        "version",
        "num_groups",
        "num_var_data_fields",
    )
    order_octets: Tuple = tuple([2 for _ in range(0, len(order))])
    TOTAL_OCTETS: int = 12

    def __init__(
        self,
        block_length: int,
        template_id: int,
        schema_id: int,
        version: int,
        num_groups: int,
        num_var_data_fields: int,
    ):
        params = [
            block_length,
            template_id,
            schema_id,
            version,
            num_groups,
            num_var_data_fields,
        ]

        for param, member, max_octets in zip(params, self.order, self.order_octets):
            if not fits_in_bytes(max_octets, param):
                raise ValueError(
                    "{} needs to fit within 2^{} but got {}".format(
                        member, max_octets, param
                    )
                )
            setattr(self, member, param)

    def serialize(self) -> bytes:
        outbuf = bytes()
        for octets, field in zip(self.order_octets, self.order):
            type_mapping: str
            if octets == 2:
                type_mapping = "<H"
            elif octets == 4:
                type_mapping = "<L"
            else:
                raise NotImplementedError

            outbuf += struct.pack(type_mapping, getattr(self, field))
        return outbuf

    @classmethod
    def deserialize(cls, byte_buffer: bytes) -> Tuple[Self, bytes]:
        if len(byte_buffer) < cls.TOTAL_OCTETS:
            raise ValueError(
                "SBE Header requires {} octets but received {}".format(
                    cls.TOTAL_OCTETS, len(byte_buffer)
                )
            )

        params = {}
        cur_pos = 0
        for num_octets, field in zip(cls.order_octets, cls.order):
            type_mapping: str
            end_pos = cur_pos + num_octets
            logging.debug(
                "deser: octets: {} field: {} cur_pos: {} len(byte_buffer): {}".format(
                    num_octets, field, cur_pos, len(byte_buffer)
                )
            )

            if num_octets == 2:
                type_mapping = "<H"
            elif num_octets == 4:
                type_mapping = "<L"
            else:
                raise NotImplementedError

            (val,) = struct.unpack(type_mapping, byte_buffer[cur_pos:end_pos])
            params.update({field: val})
            cur_pos = end_pos
        return (cls(**params), byte_buffer[cur_pos:])

    def __len__(self) -> int:
        """Size of this header in bytes. To get the size of the
        root block you'd want to call `block_length`.
        """
        return self.TOTAL_OCTETS
