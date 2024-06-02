from typing import cast, List, Tuple, Union
from joshua.fix.utils.ring_buffer import ReadonlyRingBuffer
from joshua.fix.fix_ring_buffer import FixReadonlyRingBuffer
from joshua.fix.sofh import Sofh
import logging
import multiprocessing as mp
from pathlib import Path
import datetime as dt
import polars as pl
from io import StringIO

from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.common import align_by
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory


def initialize() -> None:
    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
logger = mp_ctx.get_logger()
logger.propagate = True

logger.setLevel(logging.WARNING)
# logger.setLevel(logging.getLogger().level)
# logger.setLevel(logging.WARNING)


class StatRingBuffer:

    def __init__(self, frb: FixReadonlyRingBuffer) -> None:
        self._frb = frb

    def check_for_complete_messages(self) -> bool:
        """Assumes that find first has been properly set and starts
        reading from current file pos.
        """
        pass

        # since we are going to be sometimes > last_pos, and sometimes < last_pos
        # we need to figure out, based on our starting position, when to stop
        """
        curr_pos = rb._file_mmap().tell()
        last_pos = rb.last_pos()
        started_after_last_pos = curr_pos >= last_pos
        started_before_last_pos = curr_pos < last_pos
        """

        while True:

            (sofh, _) = Sofh.deserialize(
                byte_buffer=self._file_mmap[0 : Sofh.NUM_BYTES]  # type: ignore[attr-defined] # defined in base class
            )
            sofh = cast(Sofh, sofh)
            full_msg_sz = sofh.message_len()
            # seek current pos + full_msg_sz
            self._file_mmap.seek(full_msg_sz, 1)  # type: ignore[attr-defined] # defined in base class

    @staticmethod
    def has_wrapped(rb: ReadonlyRingBuffer) -> bool:
        has_wrapped = False
        check_size = 1500
        if (
            rb._file_mmap[0:check_size] != b"\0" * check_size
            and rb._file_mmap[-check_size:] != b"\0" * check_size
        ):
            has_wrapped = True
            mmap_len = rb.mmap_max_bytes
            start_pos = mmap_len - check_size
            last_bytes = rb._file_mmap[-check_size:].hex(sep=":", bytes_per_sep=8)
            print(
                f"last bytes of file starting at pos: {start_pos} and ending at pos: {mmap_len} = {last_bytes}"
            )
        return has_wrapped

    @classmethod
    def get_dataframes(cls, header: SplineMessageHeader, data: bytes) -> Union[
        Tuple[dt.datetime, List[str], List[pl.DataFrame], bytes],
        Tuple[dt.datetime, pl.DataFrame, bytes],
        Tuple[bytes],
    ]:
        remaining_bytes = data
        if (
            header.template_id == MuniCurvesMessageFactory.template_id
            and header.schema_id == MuniCurvesMessageFactory.schema_id
            and header.version == MuniCurvesMessageFactory.version
        ):
            # then we want to deserialize the data using a curve.
            # since we've already consumed these, we'll just use their sizes
            # so we can get any offset.
            head_len = (
                Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
            )
            head_pad = align_by(head_len, 4)
            # skip any padding for byte alignment
            remaining_bytes = remaining_bytes[head_pad:]
            (dttm, feature_sets, dfs, remaining_bytes) = (
                MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(
                    remaining_bytes
                )
            )
            return (dttm, feature_sets, dfs, remaining_bytes)
        elif (
            header.template_id == MuniYieldPredictionMessageFactory.template_id
            and header.schema_id == MuniYieldPredictionMessageFactory.schema_id
            and header.version == MuniYieldPredictionMessageFactory.version
        ):
            head_len = (
                Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
            )
            head_pad = align_by(head_len, 4)
            remaining_bytes = remaining_bytes[head_pad:]
            (dt_tm, yield_df, remaining_bytes) = (
                MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
                    remaining_bytes
                )
            )
            return (dt_tm, yield_df, remaining_bytes)
        else:
            # we don't know what it is, we'll just skip the data
            print("unknown message type, header: {header}")
            return tuple(cast(bytes, remaining_bytes))  # type: ignore[return-value]

    def read_messages(self) -> None:
        """Check if file has wrapped. If it has, start at
        last_pos+1 and find the next sofh. Continue reading
        sofh and message size until reach last_pos.
        """
        frb = self._frb
        for sofh, msg_hdr, msg_bytes in frb.messages():
            strbuf = StringIO()
            strbuf.write(f"sofh: {sofh}\n")
            strbuf.write(f"header:\n{msg_hdr}\n")
            msg = frb.get_app_message_from_bytes(data=msg_bytes, msg_hdr=msg_hdr)
            strbuf.write(f"data:\n{msg}\n")
            print(strbuf.getvalue())


def main() -> int:
    import sys

    initialize()
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.get_logger()
    logger.propagate = True

    logger.setLevel(logging.WARNING)

    print_data = True
    filename = sys.argv[1]
    file_path = Path(filename).expanduser().resolve()
    rb = FixReadonlyRingBuffer(filename=file_path)

    srb = StatRingBuffer(frb=rb)
    print(f"ring_buffer.path: {file_path}")
    print(f"ring_buffer position.path: {file_path}.pos")
    print(f"bytes_available: {srb._frb.bytes_available_to_read()}")
    print(f"file_stats:\n{file_path.stat()}")
    print(f"current last_pos: {srb._frb.last_pos}")
    # next_sofh = srb.find_next_sofh(rb)
    # print(f"first sofh found from last pos : {next_sofh}")
    # sofh_bytes = rb._file_mmap[next_sofh : next_sofh + 6]
    # print(f"6 bytes from next sofh: {sofh_bytes.hex(":")}")

    first_sofh = srb._frb.seek_next_sofh()._file_mmap.tell()
    print(f"first_sofh from 0: {first_sofh}")
    sofh_bytes = srb._frb._file_mmap[first_sofh : first_sofh + 6]
    print("6 bytes from first sofh: {}".format(sofh_bytes.hex(":")))
    has_wrapped = srb._frb.wrapped()
    print(f"file has wrapped: {has_wrapped}")

    if has_wrapped:
        last_pos = rb.last_pos
        sofh_bytes = srb._frb._file_mmap[last_pos : last_pos + 6]
        print("6 bytes after last_pos: {}".format(sofh_bytes.hex(":")))
        # see if the bytes are actually an sofh
        try:
            (sofh, _) = Sofh.deserialize(byte_buffer=rb._file_mmap[0 : Sofh.NUM_BYTES])
            sofh = cast(Sofh, sofh)
            print(f"sofh after last pos: {sofh}")
        except NotImplementedError:
            print("bytes immediately following last_pos are not a sofh")
            # try to find the next sofh
            # seek to the last pos + 1
            last_pos = rb.last_pos
            srb._frb._file_mmap.seek(last_pos + 1, 0)
            print(f"did seek to {last_pos + 1}")
            print(f"_file_mmap.tell() should match: {srb._frb._file_mmap.tell()}")
            next_sofh = srb._frb.seek_next_sofh()._file_mmap.tell()
            print(f"next_sofh from {last_pos + 1}: {next_sofh}")
            sofh_bytes = srb._frb._file_mmap[next_sofh : next_sofh + 6]
            print("6 bytes from next sofh: {}".format(sofh_bytes.hex(":")))
    if print_data:
        print("\nstart_reading_messages\n")
        srb.read_messages()
    return 0


if __name__ == "__main__":
    import sys

    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.get_logger()
    logger.propagate = True

    logger.setLevel(logging.WARNING)
    # logger.setLevel(logging.getLogger().level)
    # logger.setLevel(logging.WARNING)

    logging.getLogger().setLevel(logging.WARNING)
    sys.exit(main())
