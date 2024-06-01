import struct
from joshua.fix.sbe import sbe_little_endian

# from joshua.fix.types import SplineFixType
from dataclasses import dataclass
import logging

try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import cast, Optional, Tuple
import multiprocessing as mp

start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
logger = mp_ctx.get_logger()
logger.propagate = False
# logger.setLevel(logging.root.level)
logger.setLevel(level=logging.getLogger().level)
# logger.setLevel(logging.DEBUG)
"""
SOFH is a 4 byte length followed by a 2 byte encoding. SOFH is always in network byte
order(big endian)and is followed immediately by a message. Size does not include
the header itself, only the content of the following message.
"""


# NOTE, subclassing SplineFixType causes a circular import somehow
@dataclass
class Sofh:
    # unsigned int. 4 bytes
    size: int
    # 2 bytes, or an unsigned short
    encoding: bytes
    NUM_BYTES: int = 6

    def __init__(
        self,
        size: int = 0,
        enc: bytes = sbe_little_endian,
        size_includes_self: bool = False,
    ):
        logger.debug(f"size in is {size}")
        # need to have our own size as part of the number so we can't exceed that
        if size >= 2**32 - self.NUM_BYTES:
            logger.error(f"Sofh passed size too large. {size}")
            raise ValueError(f"{size} + {self.NUM_BYTES} is too big to fit in 2^32")
        self.size = size
        if size_includes_self is False:
            self.size += self.NUM_BYTES
        logger.debug("sofh.size set to {}".format(self.size))
        if len(enc) != 2:
            raise ValueError(f"{enc.hex(':')} needs to be exactly 2 bytes")
        self.encoding = enc

    def size_as_bytes(self) -> bytes:
        logger.debug("sofh.size_as_bytes() current self.size: {}".format(self.size))
        # L is 4 bytes
        ret = struct.pack("!L", self.size)
        return ret

    def __repr__(self) -> str:
        retstr = self.size_as_bytes().hex(":")
        retstr += ":"
        retstr += self.encoding.hex(":")
        return retstr

    def __str__(self) -> str:
        retstr = f"size: {self.size!r} encoding: {self.encoding!r}"
        return retstr

    def serialize(self) -> bytes:
        """Turn ourselves into a BIG endian array of bytes"""
        # sofh is ALWAYS network byte/big endian order
        ser_ret = self.size_as_bytes()
        # already big endian bytes, can just put on wire
        ser_ret += self.encoding
        return ser_ret

    @classmethod
    def deserialize(cls, byte_buffer: bytes) -> Tuple[Self, bytes]:
        # size always comes first, and sofh is always network byte order
        (size,) = struct.unpack("!L", byte_buffer[:4])
        # logger.debug("Sofh.deserialize read length of: {}".format(size))
        # already in nbo and we're just going to use as is
        encoding = byte_buffer[4:6]
        if encoding != sbe_little_endian:
            raise NotImplementedError("Only support sbe little endian encoding")

        # we're subtracting the NUM_BYTES because the constructor
        # will automatically add them and they're already included
        return (cls(size=size, enc=encoding, size_includes_self=True), byte_buffer[6:])

    def __len__(self) -> int:
        """The SOFH itself is always 6 bytes."""
        return self.NUM_BYTES

    def message_len(self) -> int:
        """Message len is INCLUSIVE of the SOFH and represents the full
        number of bytes for the message as a whole, from the beginning
        of SOFH to the last byte of the message. By reading this number
        of bytes from a buffer the next position should be at the start
        of the next SOFH.
        """
        return self.size

    def message_body_len(self) -> int:
        """Length EXCLUSIVE of sofh.
        When reading from a data buffer, the way to get a message will be:
        Reading 6 bytes and deserializing to this object, then, getting the full
        size, and then reading the rest of the data. This makes the full message_len()
        not as useful because the caller needs to know to subtract the length
        of this object from it. This is a convienence function provided to direct
        the caller how many bytes to read next.
        """
        return self.message_len() - self.NUM_BYTES

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES

    @classmethod
    def find_sofh(
        cls, data: bytes, encoding: bytes = sbe_little_endian
    ) -> Optional[int]:
        if data is None:
            return None
        pos = data.find(encoding)
        """# commented out because could be a 40-100MB buffer
        logger.debug(
            f"{pos} = {data!r}.find({encoding!r}) struct_size_bytes={cls.struct_size_bytes()}"
        )
        """
        if pos == -1:
            return None
        else:
            # returns the position of the encoding, and we have 4 bytes of size preceding that
            logger.debug(
                f"pos={pos} - struct_size_bytes() - len(encoding): {cls.struct_size_bytes() - len(encoding)}"
            )
            size_field_len = cls.struct_size_bytes() - len(encoding)
            logger.debug(
                f"{size_field_len} = {cls.struct_size_bytes()} - {len(encoding)}"
            )
            logger.debug(
                f"returning sofh at {pos - size_field_len} = {pos} - {size_field_len}"
            )
            return pos - size_field_len

    @classmethod
    def count_sofh(
        cls, data: bytes, encoding: bytes = sbe_little_endian
    ) -> Optional[int]:
        # find the first one
        first = cls.find_sofh(data, encoding)
        if first is None:
            return first
        # we have at least one. Deser.
        start_pos = int(first)
        length = len(data)
        count = 0
        while start_pos < length:
            try:
                (sofh, _) = Sofh.deserialize(data[start_pos:])
                sofh = cast(Sofh, sofh)
                start_pos += sofh.message_len()
                count += 1
            except Exception as ex:
                logger.debug(f"got exception while processing count_sofh. {ex}")
        logger.debug(f"count_sofh -> {count}")
        return count
