from typing import Any, cast, Dict, Generator, Optional, Tuple
from joshua.fix.sofh import Sofh
import logging
import polars as pl


from joshua.fix.utils.ring_buffer import ReadonlyRingBuffer
from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory


def initialize() -> None:

    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


class FixReadonlyRingBuffer(ReadonlyRingBuffer):
    def wrapped(self) -> bool:
        if self._wrapped:
            return self._wrapped
        check_size = 1500
        if (
            self._file_mmap[0:check_size] != b"\0" * check_size
            and self._file_mmap[-check_size:] != b"\0" * check_size
        ):
            self._wrapped = True
            """
            mmap_len = self.mmap_max_bytes
            start_pos = mmap_len - check_size
            last_bytes = self._file_mmap[-check_size:].hex(sep=":", bytes_per_sep=8)
            print(
                f"last bytes of file starting at pos: {start_pos} and ending at pos: {mmap_len} = {last_bytes}"
            )
            """
        return self._wrapped

    @staticmethod
    def read_header(data: bytes) -> Tuple[SplineMessageHeader, bytes]:
        (msg_hdr, rem_bytes) = SplineMessageHeader.deserialize(data)
        return (msg_hdr, rem_bytes)

    def seek_next_sofh(self) -> ReadonlyRingBuffer:
        """Using the current file position, search
        for the next sofh, leaving the file pointer
        at that position.
        """
        last_written_pos = self.last_pos
        # print(f"last_pos={last_written_pos}")
        curr_pos = self._file_mmap.tell()
        # print(f"curr_pos={curr_pos}")
        if last_written_pos == curr_pos:
            # print("last_pos == curr_pos leaving alone")
            return self
        # find the next sofh
        sofh_size = Sofh.struct_size_bytes()
        interval = 1500
        bytes_avail = self.bytes_available_to_read()
        if bytes_avail < sofh_size:
            self._file_mmap.seek(last_written_pos, 0)
            print("not many bytes available, seeking last_pos")
            return self
        potential_sofh_bytes = self.read(sofh_size + interval)
        sofh_pos = Sofh.find_sofh(potential_sofh_bytes)
        bytes_avail -= len(potential_sofh_bytes)
        while sofh_pos is None and bytes_avail > 0:
            curr_pos = self._file_mmap.tell()
            next_amt = max(sofh_size + interval, bytes_avail)
            # we want to back up one sofh_size in case we're mid
            # header (probably only need sofh_size-1) and
            # then we'll add the next chunk
            potential_sofh_bytes = potential_sofh_bytes[
                len(potential_sofh_bytes) - sofh_size :
            ]
            next_bytes = self.read(next_amt)
            bytes_avail -= len(next_bytes)
            potential_sofh_bytes += next_bytes
            sofh_pos = Sofh.find_sofh(potential_sofh_bytes)

        if sofh_pos is not None:
            # print(f"found next_sofh at {curr_pos}+{sofh_pos}, seeking to next_sofh")
            self._file_mmap.seek(curr_pos + sofh_pos, 0)
        else:
            # we didn't find anything, start at last_pos
            print(f"did not find next_sofh, seeking to last_pos={self.last_pos}")
            self._file_mmap.seek(self.last_pos, 0)
        return self

    def get_message_body(self, sofh: Sofh) -> Optional[bytes]:
        """Given an Sofh, takes the number of bytes
        in the message excluding sofh, and retrieves
        them. Should include the header and message body.
        """
        # how long is the remaining parts to read?
        body_len = sofh.message_body_len()
        logging.debug(f"message_body_len={body_len}")
        # read that data.
        try:
            return self.read(body_len)
        except Exception as ex:
            logging.error(f"error trying to read message_body: {ex}")
        return None

    def get_sofh(self) -> Optional[Sofh]:
        sofh_bytes = self.read(Sofh.NUM_BYTES)
        try:
            (sofh, _) = Sofh.deserialize(sofh_bytes)
            sofh = cast(Sofh, sofh)
            return sofh
        except Exception as ex:
            logging.error(f"error trying to read sofh: {ex}")
        return None

    def messages(
        self,
    ) -> Generator[Tuple[Sofh, SplineMessageHeader, bytes], None, None]:
        """Starting at the current position, read sofh.num_bytes,
        see if not a header, seek next header. Read header, get
        message bytes, read message bytes, yield entire message.
        """
        logging.debug(".messages()")
        try:
            logging.debug("top of try")
            curr_pos = self._file_mmap.tell()
            logging.debug("after try")
            self.seek_next_sofh()
            logging.debug("after.seek_next_sofh")
            next_sofh: Optional[int] = self._file_mmap.tell()
            logging.debug(f"next_sofh is {next_sofh}")
        except Exception as ex:
            logging.error(f"did not find sofh. {next_sofh}. {ex}")
            return
        if next_sofh is None:
            # something is probably wrong, or the file is empty
            logging.debug("nothing to see here")
            return
        # else, we have our start
        logging.debug(f"next_sofh is {next_sofh}")
        while next_sofh is not None:
            logging.debug(f"next_sofh is not None: {next_sofh}")
            sofh_fpos = next_sofh
            self._file_mmap.seek(sofh_fpos, 0)
            # get the sofh
            sofh = self.get_sofh()
            logging.debug(f"got sofh: {sofh}")
            if sofh is None:
                return
            logging.debug("get_message_body")
            body_bytes = self.get_message_body(sofh)
            logging.debug("message body: {}".format(body_bytes.hex(":")))  # type: ignore[union-attr]
            # should start with the header
            logging.debug("read_header")
            (header, rem_bytes) = self.read_header(data=body_bytes)  # type: ignore[arg-type]
            logging.debug("read_header now yield triplet")
            yield (sofh, header, rem_bytes)
            logging.debug("returned from yield")
            # set to end of msg and look for next sofh
            # where we were last
            # message_len includes the full sofh header
            curr_pos = sofh_fpos + sofh.message_len()
            curr_pos = self.calc_absolute_seek_pos(curr_pos)
            self._file_mmap.seek(curr_pos)
            bytes_avail = self.bytes_available_to_read()
            if bytes_avail > Sofh.NUM_BYTES:
                next_sofh = self.seek_next_sofh()._file_mmap.tell()
            else:
                next_sofh = None

    def analyze(self) -> Dict[str, Any]:
        curve_metrics = {"total_messages": 0, "feature_sets": set()}
        curve_metrics["total_rows"] = 0
        curve_metrics["max_total_message_bytes"] = 0
        curve_metrics["min_total_message_bytes"] = 65536
        curve_metrics["min_body_message_bytes"] = 65536
        curve_metrics["max_body_message_bytes"] = 0
        curve_metrics["total_bytes"] = 0

        prediction_metrics = {}
        prediction_metrics = {"total_messages": 0}
        prediction_metrics["total_rows"] = 0
        prediction_metrics["max_total_message_bytes"] = 0
        prediction_metrics["min_total_message_bytes"] = 65536
        prediction_metrics["min_body_message_bytes"] = 65536
        prediction_metrics["max_body_message_bytes"] = 0
        prediction_metrics["total_bytes"] = 0
        for sofh, head, data in self.messages():
            if data is None:
                break
            # what type of message?
            template_id = head.template_id
            schema_id = head.schema_id
            version = head.version
            body_len = len(data)
            match (template_id, schema_id, version):
                case (
                    MuniCurvesMessageFactory.template_id,
                    MuniCurvesMessageFactory.schema_id,
                    MuniCurvesMessageFactory.version,
                ):
                    tb = sofh.message_len()
                    logging.debug("analyze curves")
                    curve_metrics["total_bytes"] = (
                        cast(int, curve_metrics["total_bytes"]) + tb
                    )
                    (curve_dt_tm, feature_list, df_list, rem_bytes) = (
                        MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(
                            data
                        )
                    )
                    curve_interval_timestamp = int(curve_dt_tm.timestamp())
                    curve_ts = curve_metrics.get("timestamps")
                    if curve_ts is None:
                        curve_ts = {f"{curve_interval_timestamp}": 0}
                    curve_ts[f"{curve_interval_timestamp}"] = (
                        curve_ts[f"{curve_interval_timestamp}"] + 1
                    )
                    curve_metrics["timestamps"] = curve_ts
                    for feat, df in zip(feature_list, df_list):
                        # each dataframe is one curve, one row basically
                        curve_metrics["total_messages"] = (
                            cast(int, curve_metrics["total_messages"]) + 1
                        )
                        curve_metrics["total_rows"] = (
                            cast(int, curve_metrics["total_rows"]) + df.shape[0]
                        )
                        cast(set, curve_metrics["feature_sets"]).add(feat)
                        curve_metrics["max_total_message_bytes"] = max(
                            cast(int, curve_metrics["max_total_message_bytes"]),
                            sofh.message_len(),
                        )
                        curve_metrics["max_body_message_bytes"] = max(
                            cast(int, curve_metrics["max_body_message_bytes"]), body_len
                        )
                        curve_metrics["min_total_message_bytes"] = min(
                            cast(int, curve_metrics["min_total_message_bytes"]),
                            sofh.message_len(),
                        )
                        curve_metrics["min_body_message_bytes"] = min(
                            cast(int, curve_metrics["min_body_message_bytes"]), body_len
                        )

                case (
                    MuniYieldPredictionMessageFactory.template_id,
                    MuniYieldPredictionMessageFactory.schema_id,
                    MuniYieldPredictionMessageFactory.version,
                ):
                    logging.debug(f"body_len = {body_len}")
                    tb = sofh.message_len()
                    logging.debug("analyze predictions")
                    prediction_metrics["total_bytes"] = (
                        prediction_metrics["total_bytes"] + tb
                    )
                    (prediction_dt_tm, df, rem_bytes) = (
                        MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
                            data
                        )
                    )
                    prediction_interval_timestamp = int(prediction_dt_tm.timestamp())
                    prediction_ts = prediction_metrics.get("timestamps")
                    if prediction_ts is None:
                        prediction_ts = {f"{prediction_interval_timestamp}": 0}
                    prediction_ts[f"{prediction_interval_timestamp}"] = (
                        prediction_ts[f"{prediction_interval_timestamp}"] + 1
                    )
                    prediction_metrics["timestamps"] = prediction_ts
                    prediction_metrics["total_messages"] = (
                        prediction_metrics["total_messages"] + 1
                    )
                    prediction_metrics["total_rows"] = (
                        prediction_metrics["total_rows"] + df.shape[0]
                    )
                    prediction_metrics["max_body_message_bytes"] = max(
                        prediction_metrics["max_body_message_bytes"], body_len
                    )
                    prediction_metrics["min_body_message_bytes"] = min(
                        prediction_metrics["min_body_message_bytes"], body_len
                    )
                    prediction_metrics["max_total_message_bytes"] = max(
                        prediction_metrics["max_total_message_bytes"],
                        sofh.message_len(),
                    )
                    prediction_metrics["min_total_message_bytes"] = min(
                        prediction_metrics["min_total_message_bytes"],
                        sofh.message_len(),
                    )
                case _:
                    logging.error("unknown message type: {header}")
        return {
            "curve_metrics": curve_metrics,
            "prediction_metrics": prediction_metrics,
        }

    @classmethod
    def get_app_message_from_bytes(
        cls, data: bytes, msg_hdr: Optional[SplineMessageHeader] = None
    ) -> Optional[Dict[str, Any]]:
        if data is None:
            return None
        # what type of message?
        if msg_hdr is None:
            (head, rem_bytes) = cls.read_header(data)
        else:
            head = msg_hdr
            rem_bytes = data
        template_id = head.template_id
        schema_id = head.schema_id
        version = head.version
        match (template_id, schema_id, version):
            case (
                MuniCurvesMessageFactory.template_id,
                MuniCurvesMessageFactory.schema_id,
                MuniCurvesMessageFactory.version,
            ):
                (curve_dt_tm, feature_list, df_list, rem_bytes) = (
                    MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(data)
                )
                return {
                    "msg_name": "MuniCurve",
                    "interval": curve_dt_tm,
                    "feature_list": feature_list,
                    "df_list": df_list,
                    "rem_bytes": rem_bytes,
                }
            case (
                MuniYieldPredictionMessageFactory.template_id,
                MuniYieldPredictionMessageFactory.schema_id,
                MuniYieldPredictionMessageFactory.version,
            ):
                (prediction_dt_tm, df, rem_bytes) = (
                    MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
                        data
                    )
                )
                return {
                    "msg_name": "MuniYieldPrediction",
                    "interval": prediction_dt_tm,
                    "df": df,
                }
            case _:
                logging.error("unknown message type. please add to fn: {msg_hdr}")
                return {"msg_name": "unknown", "msg_hdr": msg_hdr, "data": data}

        return None
