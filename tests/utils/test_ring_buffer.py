import pytest
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile
from io import BytesIO
import mmap

from joshua.core.utils.ring_buffer import WritableRingBuffer, ReadonlyRingBuffer


def test_writable_ring_buffer_init() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writeable_ring_buffer_init -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 1024
    sizeof_pos = 8
    wrb = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)

    assert mmap_file_path.exists()
    assert mmap_file_pos_path.exists()
    mmap_stat = mmap_file_path.stat()
    assert mmap_stat.st_size == num_bytes
    mmap_pos_stat = mmap_file_pos_path.stat()
    assert mmap_pos_stat.st_size == sizeof_pos

    assert wrb.mmap_max_bytes == num_bytes
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()


def test_writeable_ring_buffer_init_fail() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writeable_ring_buffer_init_fail -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 1025
    with pytest.raises(ValueError) as exc:
        WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)
    assert str(exc.value) == "size_in_bytes must be 0 or a larger power of 2"
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()


def test_writeable_ring_buffer_split_write() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writeable_ring_buffer_split_write -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 1024
    wrb = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)

    write_size = 1
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 1
    assert wsplit[0] == 1

    write_size = 1023
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 1
    assert wsplit[0] == 1023

    write_size = 1024
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 1
    assert wsplit[0] == 1024

    write_size = 1025
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 2
    assert wsplit[0] == 1024
    assert wsplit[1] == 1

    write_size = 2432
    splits = [1024, 1024, 384]
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 3
    for ws, sl in zip(wsplit, splits):
        assert ws == sl

    # now with an offset
    wrb.last_pos = 800
    write_size = 2432
    splits = [224, 1024, 1024, 160]
    wsplit = wrb.split_write(write_size)
    assert len(wsplit) == 4
    for ws, sl in zip(wsplit, splits):
        assert ws == sl
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()


def test_writeable_ring_buffer_write() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writeable_ring_buffer_write -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 1024
    wrb = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)

    strings = [
        "hello world!",
        "supercalifragilisticxpaladocious",
        "abcdefghijklmnopqrstuvwxyz" * 100,
    ]

    last = 0
    for w in strings:
        last += len(w)
        last = last % num_bytes
        bytes_written = wrb.write(w.encode("latin1"))
        assert bytes_written == len(w)
        assert wrb.last_pos == last

    bb = BytesIO()
    tst_mmap = mmap.mmap(mmap_file.fileno(), 0)
    bb.write(tst_mmap[0:])
    assert num_bytes == len(bb.getvalue())
    big_string = bb.getvalue().decode("latin1")
    cat_strs = "".join(strings)
    div, mod = divmod(len(cat_strs), num_bytes)
    pos = div * num_bytes
    assert pos == len(cat_strs) - mod
    ansbuf = BytesIO()
    ansbuf.write(cat_strs[pos:].encode("latin1"))
    # then we need to make up the difference
    tail = pos - (num_bytes - mod)
    ansbuf.write(cat_strs[tail:pos].encode("latin1"))
    logging.debug(f"type(ansbuf.getvalue())={type(ansbuf.getvalue())}")
    logging.debug(f"type(big_string)={type(big_string)}")

    assert big_string.encode("latin1") == ansbuf.getvalue()
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()


def test_readonly_ring_buffer_read() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writeable_ring_buffer_write -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 2048
    wrb = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)
    rrb = ReadonlyRingBuffer(filename=mmap_file_path)

    strings = [
        "hello world!",
        "supercalifragilisticxpaladocious",
        "abcdefghijklmnopqrstuvwxyz" * 50,
        "abcdefghijklmnopqrstuvwxyz" * 50,
    ]

    wrb_all_written = BytesIO()
    rrb_all_read = BytesIO()

    last = 0
    # want to offset by 1 to stagger the reads and writes slightly
    for i, w in enumerate(strings):
        last += len(w)
        last = last % num_bytes
        wrb.write(w.encode("latin1"))
        wrb_all_written.write(w.encode("latin1"))
        if i > 0:
            rrb_all_read.write(rrb.read())

    if rrb.bytes_available_to_read() > 0:
        logging.debug("more bytes to read after loop")
        rrb_all_read.write(rrb.read())

    logging.debug(f"bytes written: {len(wrb_all_written.getvalue())}")
    logging.debug(f"bytes read: {len(rrb_all_read.getvalue())}")
    logging.debug(
        f"wrb_all_written: rrb_all_read:\n{wrb_all_written.getvalue()!r}\n{rrb_all_read.getvalue()!r}"
    )
    assert wrb_all_written.getvalue() == rrb_all_read.getvalue()

    assert rrb.bytes_available_to_read() == 0
    assert rrb.last_pos == wrb.last_pos
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()


def test_writable_ring_buffer_write_and_reopen() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_writable_ring_buffer_write_and_reopen -o log_cli=true
    """
    mmap_file = NamedTemporaryFile(mode="w+b")
    mmap_file_path = Path(mmap_file.name)
    mmap_file_pos_path = mmap_file_path.with_suffix(".pos")
    num_bytes = 256
    wrb = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)

    strings = [
        "hello world!",
        "supercalifragilisticxpaladocious",
        "abcdefghijklmnopqrstuvwxyz" * 100,
    ]

    all_bytes_written = BytesIO()
    last = 0
    for w in strings:
        last += len(w)
        last = last % num_bytes
        bytes_written = wrb.write(w.encode("latin1"))
        all_bytes_written.write(w.encode("latin1"))
        assert bytes_written == len(w)
        assert wrb.last_pos == last

    # now close file
    del wrb

    # now open the file
    wrb2 = WritableRingBuffer(filename=mmap_file_path, size_in_bytes=num_bytes)
    assert wrb2.last_pos == last
    write_this = "".join([str(x) for x in range(0, 100, 1)])
    wrb2.write(write_this.encode("latin1"))
    all_bytes_written.write(write_this.encode("latin1"))
    last_now = wrb2.last_pos
    assert last_now == (len(all_bytes_written.getvalue())) % num_bytes

    buflen = len(all_bytes_written.getvalue())
    logging.debug(f"buflen={buflen}")
    buflen_div, buflen_mod = divmod(buflen, num_bytes)
    logging.debug(f"divmod(buflen)={buflen_div}, {buflen_mod}")
    # get the front
    buffer_bytes = all_bytes_written.getvalue()[num_bytes * buflen_div :]
    # get the tail
    buf_end = num_bytes * buflen_div - 1
    buffer_bytes += all_bytes_written.getvalue()[-num_bytes : buf_end + 1]
    bb = BytesIO()
    tst_mmap = mmap.mmap(mmap_file.fileno(), 0)
    bb.write(tst_mmap[0:])
    assert buffer_bytes == bb.getvalue()
    if mmap_file_pos_path.exists():
        mmap_file_pos_path.unlink()
