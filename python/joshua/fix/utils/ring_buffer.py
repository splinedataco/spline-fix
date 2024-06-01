from typing import Any, List, Optional, Union
import mmap
from pathlib import Path
import struct
from math import log2
from io import BytesIO


class WritableRingBuffer:
    """Limitations/features:
    Single producer only. If multiple producers they will overwrite each other.
    Multiple readers. Supports 0-n readers. Each reader keeps own location in their read fd.
    close operation is only by deleting the Object.
    """

    def __init__(self, filename: Union[Path, str], size_in_bytes: int = 0) -> None:
        if filename is None:
            raise ValueError("filename must not be None")
        file_path = Path(filename).expanduser()

        l2 = log2(size_in_bytes)
        if size_in_bytes < 0 or l2 != int(l2):
            raise ValueError("size_in_bytes must be 0 or a larger power of 2")
        elif size_in_bytes == 0 and not file_path.exists():
            raise ValueError(
                f"size_in_bytes may only be 0 if file exists. {file_path}.exists() == False. Provide max file size."
            )
        if not file_path.exists():
            with file_path.open(mode="w+b") as f:
                f.seek(size_in_bytes)
        if not file_path.is_file():
            raise ValueError(f"{file_path}.is_file() == False")
        file_stats = file_path.stat()
        file_size = file_stats.st_size
        if file_size != 0 and file_size != size_in_bytes and size_in_bytes != 0:
            raise ValueError(
                f"File '{file_path}' already exists and has different size. file: {file_size} != {size_in_bytes}. Use size = 0 to use existing file"
            )
        if file_size == 0 and size_in_bytes == 0:
            raise ValueError(
                f"File '{file_path}' already exists but both file and size_in_bytes are 0."
            )

        # if file doesn't exist create it and the size that we requested
        if not file_path.exists() or file_size == 0:
            with open(file_path, "w+b") as f:
                f.seek(size_in_bytes - 1, 0)
                f.write(b"\0")
                f.flush()
        file_stats = file_path.stat()
        self._file_size = file_stats.st_size
        self._file_handle = open(file_path, "r+b")
        self._file_path = file_path
        self._file_mmap = mmap.mmap(
            self._file_handle.fileno(),
            0,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_WRITE | mmap.PROT_READ,
        )
        # need a mmap that will keep the furthest location we've written to
        # position map
        file_pos_path = self._file_path.with_suffix(".pos")
        if not file_pos_path.exists():
            with open(file_pos_path, "w+b") as f:
                f.write(struct.pack("<Q", 0))
                f.flush()
        self._file_pos_path = file_pos_path
        self._file_pos_handle = open(file_pos_path, "r+b")
        self._file_pos_mmap = mmap.mmap(
            self._file_pos_handle.fileno(),
            0,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_WRITE | mmap.PROT_READ,
        )
        self._pos_size = struct.calcsize("Q")
        (last_pos,) = struct.unpack("<Q", self._file_pos_mmap[0 : self._pos_size])
        self._file_mmap.seek(last_pos, 0)

    def split_write(self, size: int) -> List[int]:
        ret_list: List[int] = list()
        # first, figure out how far we are from the end
        next_max = self.mmap_max_bytes - self.last_pos
        remaining_bytes = size
        if size <= next_max:
            ret_list.append(size)
            return ret_list
        else:
            ret_list.append(next_max)
            remaining_bytes -= next_max

        ret_list.extend(
            [self.mmap_max_bytes] * (remaining_bytes // self.mmap_max_bytes)
        )
        remainder = remaining_bytes % self.mmap_max_bytes
        if remainder > 0:
            ret_list.append(remainder)
        return ret_list

    @property
    def mmap_max_bytes(self) -> int:
        return self._file_size

    @mmap_max_bytes.setter
    def mmap_max_bytes(self, value: Any) -> None:
        raise NotImplementedError

    @property
    def last_pos(self) -> int:
        (last_pos,) = struct.unpack("<Q", self._file_pos_mmap[0 : self._pos_size])
        return last_pos

    @last_pos.setter
    def last_pos(self, value: int) -> None:
        if not isinstance(value, int):
            raise NotImplementedError
        if value > self.mmap_max_bytes:
            value = value % self.mmap_max_bytes
        self._file_pos_mmap.seek(0, 0)
        self._file_pos_mmap.write(struct.pack("<Q", value))
        # not sure if this is needed. We shouldn't be buffering so it
        # should be direct io
        # self._file_pos_mmap.flush(offset=-self._pos_size, size=self._pos_size)

    def update_last_pos(self) -> int:
        """Takes a tell from the mmap, writes that value
        to the pos mmap and returns the position
        """
        lp = self._file_mmap.tell()
        self.last_pos = lp
        return lp

    def write(self, data: bytes) -> int:
        remaining_data = data
        write_splits = self.split_write(len(data))
        for split in write_splits:
            self._file_mmap.write(remaining_data[0:split])
            remaining_data = remaining_data[split:]
            pos = self._file_mmap.tell()
            new_pos = pos % self.mmap_max_bytes
            if pos != new_pos:
                self._file_mmap.seek(new_pos, 0)

        # done writing out update the shared last pos
        self.last_pos = self._file_mmap.tell()
        return len(data)


class ReadonlyRingBuffer:
    def __init__(self, filename: Union[Path, str]) -> None:
        if filename is None:
            raise ValueError("filename must not be None")
        file_path = Path(filename).expanduser()

        if not file_path.exists():
            raise ValueError(f"{file_path}.exists() == False")

        if not file_path.is_file():
            raise ValueError(f"{file_path}.is_file() == False")
        file_stats = file_path.stat()
        file_size = file_stats.st_size
        if file_size == 0:
            raise ValueError(f"File '{file_path}' already exists but has 0 size.")

        file_stats = file_path.stat()
        self._file_size = file_stats.st_size
        self._file_handle = open(file_path, "rb")
        self._file_path = file_path
        self._file_mmap = mmap.mmap(
            self._file_handle.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ
        )
        # need a mmap that will keep the furthest location we've written to
        # position map
        file_pos_path = self._file_path.with_suffix(".pos")
        self._file_pos_path = file_pos_path
        if not file_pos_path.exists():
            raise ValueError(
                f"{self._file_path} exists but {self._file_pos_path} does not"
            )
        self._file_pos_handle = open(file_pos_path, "rb")
        self._file_pos_mmap = mmap.mmap(
            self._file_pos_handle.fileno(),
            0,
            flags=mmap.MAP_SHARED,
            prot=mmap.PROT_READ,
        )

        self._pos_size = struct.calcsize("Q")
        (self._last_pos,) = struct.unpack("<Q", self._file_pos_mmap[0 : self._pos_size])
        # NOTE: Our tell() pointer will be pointing to the start of the mmap.
        # so it behooves us to find the first sofh after the last_pointer and use that
        # as our start point.
        # But since this is a genric class, that will need to be done in the application
        # or a subclass
        self._wrapped = False
        self.wrapped

    @property
    def mmap_max_bytes(self) -> int:
        return self._file_size

    @mmap_max_bytes.setter
    def mmap_max_bytes(self, value: Any) -> None:
        raise NotImplementedError

    @property
    def last_pos(self) -> int:
        (last_pos,) = struct.unpack("<Q", self._file_pos_mmap[0 : self._pos_size])
        return last_pos

    @last_pos.setter
    def last_pos(self, value: int) -> None:
        raise NotImplementedError

    def wrapped(self) -> bool:
        if self._wrapped is True:
            return self._wrapped
        check_size = 4096
        if (
            self._file_mmap[0:check_size] != b"\0" * check_size
            and self._file_mmap[-check_size:] != b"\0" * check_size
        ):
            self._wrapped = True
        return self._wrapped

    def bytes_available_to_read(self) -> int:
        our_pos = self._file_mmap.tell()
        last_pos = self.last_pos
        if not self.wrapped():
            # then it's just the distance between
            avail = last_pos - our_pos
            if avail >= 0:
                return avail
        # otherwise, we've wrapped
        # if our current_pos < last_pos, then it's the distance between
        if our_pos <= last_pos:
            return last_pos - our_pos
        # otherwise, we've wrapped, and we're greater than, we need to wrap
        # num_bytes from the end of the file to our position is available
        avail = self.mmap_max_bytes - our_pos
        # and then from the start (0) to the last_pos
        avail += last_pos
        return avail

    def read(self, size: Optional[int] = None) -> bytes:
        """Max bytes or as many as are available if None."""
        max_bytes = size
        if max_bytes is None:
            # mmap_max_bytes is the file size
            max_bytes = self.bytes_available_to_read()
        else:
            max_bytes = min(max_bytes, self.bytes_available_to_read())

        # max_bytes is how many bytes to read, or how many bytes are available to read
        bb = BytesIO()
        rem_bytes = min(self.bytes_available_to_read(), max_bytes)
        our_pos = self._file_mmap.tell()
        last_pos = self.last_pos
        if our_pos > last_pos:
            # then read until eof and then start over
            bytes_to_read = min(self.mmap_max_bytes - our_pos, max_bytes)
            num = bb.write(self._file_mmap.read(bytes_to_read))
            rem_bytes -= num
            if self._file_mmap.tell() == self.mmap_max_bytes:
                self._file_mmap.seek(0, 0)
            if rem_bytes == 0:
                return bb.getvalue()
            num = bb.write(self._file_mmap.read(rem_bytes))
            rem_bytes -= num
            our_pos = self._file_mmap.tell()
        if our_pos < last_pos and rem_bytes > 0:
            if our_pos + rem_bytes > last_pos:
                raise IOError(
                    "After read wrapped end of file tried to read more than file!"
                )
            num = bb.write(self._file_mmap.read(rem_bytes))
            rem_bytes -= num
            our_pos = self._file_mmap.tell()
        if our_pos == last_pos:
            return bb.getvalue()
        if rem_bytes != 0:
            raise IOError("We've done our math wrong on reading!")
        return bb.getvalue()

    def calc_absolute_seek_pos(self, seek_pos: int) -> int:
        max_pos = self.mmap_max_bytes
        return seek_pos % max_pos
