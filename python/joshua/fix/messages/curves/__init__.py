try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any, cast, Generator, List, Tuple, Union
from array import array
import ctypes
from io import BytesIO, StringIO
import struct
import logging
import datetime as dt
import polars as pl
from collections import OrderedDict
from joshua.fix.types import GroupSizeEncodingSimple, SplineDateTime
from joshua.fix.messages.featureset import FeatureSet
from joshua.fix.messages import SplineFixType, SplineMessageHeader
from joshua.fix.data_dictionary import (
    SPLINE_MUNI_DATA_SCHEMA_ID,
    SPLINE_MUNI_DATA_SCHEMA_VERSION,
)
from joshua.fix.sofh import Sofh
from joshua.fix.common import (
    align_by,
    SplineMuniFixType,
    yield_float_to_fixed_decimal,
    yield_fixed_decimal_to_float,
)

"""NOTES: While looking at predictions, specifically:
```python
predictions_lf = pl.scan_parquet("~/s3/combined/predictions/2024/02/01/08/00/predictions.parquet")
predictions_lf.collect().describe()
Out[114]:
shape: (9, 6)
┌────────────┬───────────┬───────────────────┬───────────┬────────────┬────────────┐
│ describe   ┆ cusip     ┆ par_traded_bucket ┆ bid_yield ┆ mid_yield  ┆ ask_yield  │
│ ---        ┆ ---       ┆ ---               ┆ ---       ┆ ---        ┆ ---        │
│ str        ┆ str       ┆ f64               ┆ f64       ┆ f64        ┆ f64        │
╞════════════╪═══════════╪═══════════════════╪═══════════╪════════════╪════════════╡
│ count      ┆ 3167520   ┆ 3.16752e6         ┆ 3.16752e6 ┆ 3.16752e6  ┆ 3.16752e6  │
│ null_count ┆ 0         ┆ 0.0               ┆ 0.0       ┆ 0.0        ┆ 0.0        │
│ mean       ┆ null      ┆ 533333.333333     ┆ 3.807816  ┆ 3.693904   ┆ 3.579992   │
│ std        ┆ null      ┆ 368178.758691     ┆ 0.609902  ┆ 0.583234   ┆ 0.574411   │
│ min        ┆ 000369EV6 ┆ 100000.0          ┆ -47.75143 ┆ -49.060167 ┆ -50.368903 │
│ 25%        ┆ null      ┆ 100000.0          ┆ 3.400364  ┆ 3.301409   ┆ 3.193243   │
│ 50%        ┆ null      ┆ 500000.0          ┆ 3.837022  ┆ 3.7076     ┆ 3.571477   │
│ 75%        ┆ null      ┆ 1e6               ┆ 4.228073  ┆ 4.089478   ┆ 3.949947   │
│ max        ┆ 999999QJ7 ┆ 1e6               ┆ 61.306472 ┆ 56.7547    ┆ 52.202927  │
└────────────┴───────────┴───────────────────┴───────────┴────────────┴────────────┘
```
It became aparent that since we were sending the history of all time, we would then
have yields > abs(32.768).
Increasing to the next power of 2, 2^(32-1) gives us 2bn whereas 2^(16-1) is 32k.
The increase doubles the size per row and will drop the number of curves per mtu
from 20 to 10.

One approach would be to send an extended message, (can't be an optional field
because the yields need to be unsigned in order to get the extra pu) which is a 32bit bitfield, with 30 populated
values. The yields are changed to received, the yields are converted as before (but unsigned)
and then multiplied * -1 where the sign bit in the bitfield is high.

Or, just send 2^(32-1) values and accept that there are less per mtu, but only need to
send the few that are outside that range. Then can also handle numbers much larger than
+/-64.536.
"""


class MuniCurve(SplineFixType):
    NUM_YIELDS = 30
    features: FeatureSet
    values: List[ctypes.c_short]
    max_values: int = NUM_YIELDS
    FIXED_DECIMAL = -3

    def __init__(self, features: FeatureSet, values: Union[list[float], list[int]]):
        if FeatureSet is None:
            raise ValueError("MuniCurve requires a valid FeatureSet. Got: None")
        self.features = features
        # we are going to be using a decimal encoding with a fixed
        # exponent of FIXED_DECIMAL
        if not isinstance(values, list):
            raise NotImplementedError
        if len(values) != self.NUM_YIELDS:
            raise ValueError(
                "yield array must be exactly {} elements got {} {}'s".format(
                    self.NUM_YIELDS, len(values), type(values[0])
                )
            )
        if isinstance(values[0], int):
            logging.debug("making municurve from list of ints")
            self.values = [ctypes.c_short(int(x)) for x in values]
        elif isinstance(values[0], float):
            logging.debug("making municurve from list of floats")
            self.values = [
                ctypes.c_short(
                    yield_float_to_fixed_decimal(value=v, fixed_exp=self.FIXED_DECIMAL)
                )
                for v in values
            ]
        else:
            logging.error(
                "MuniCurve doesn't know how to build from a value of: {}".format(values)
            )
            raise NotImplementedError
        logging.debug("self.values={}".format(self.values))

    def values_as_float(self) -> List[float]:
        v_as_f = [
            yield_fixed_decimal_to_float(mantissa=x, fixed_exp=self.FIXED_DECIMAL)
            for x in self.values
        ]
        return v_as_f

    @classmethod
    def struct_size_bytes(cls) -> int:
        block_len = FeatureSet.struct_size_bytes()
        sshort_size = array("h").itemsize
        block_len += sshort_size * cls.max_values
        return block_len

    @property
    def block_length(self) -> int:
        return self.struct_size_bytes()

    def __str__(self) -> str:
        outbuf = StringIO()
        outbuf.write(str(self.features))
        outbuf.write(":\n")
        outbuf.write("tenor|value\n")
        outbuf.write("-----|-----\n")
        values_as_float = self.values_as_float()
        for i, v in enumerate(values_as_float):
            outbuf.write("{}  | ".format(float(i + 1)))
            outbuf.write("{:0.3f}".format(v))
            outbuf.write("\n")

        return outbuf.getvalue()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MuniCurve):
            return NotImplemented
        equals = True
        logging.debug("eq default of {}".format(equals))  # XXX
        equals = equals & (self.features == other.features)
        logging.debug("eq after featureset: {}".format(equals))  # XXX
        for s, o in zip(self.values, other.values):
            equals = equals & (s.value == o.value)
            if not equals:
                break
        return equals

    def serialize(self) -> bytes:
        """Turn ourselves into a little_endian array of bytes"""
        ser_ret = self.features.serialize()
        # today should always be NUM_YIELDS
        format_str = "<{}h".format(self.NUM_YIELDS)
        logging.debug(
            "serialize: type(self.values): {} format_str:'{}' values: {}".format(
                type(self.values), format_str, self.values
            )
        )
        ser_ret = ser_ret + struct.pack(format_str, *[v.value for v in self.values])
        return ser_ret

    @classmethod
    def deserialize(
        cls, data: bytes, group_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither group_type nor align_to are used"""
        logging.debug(
            "deserialize bytes in len: {} bytes: '{}'".format(len(data), data.hex())
        )
        # should start with our features
        (feats, remaining_bytes) = FeatureSet.deserialize(data)
        logging.debug("after featureset len(rem_bytes)={}".format(len(remaining_bytes)))
        # today should always be NUM_YIELDS
        num_tenors = cls.NUM_YIELDS
        format_str = "<{}h".format(num_tenors)
        sshort_size = array("h").itemsize
        sshortarray_bytes = sshort_size * num_tenors
        vals = list(struct.unpack(format_str, remaining_bytes[:sshortarray_bytes]))
        logging.debug("deserializing sshort yields: {}".format(vals))

        # return unread bytes
        remaining_bytes = remaining_bytes[sshortarray_bytes:]

        return (cls(feats, vals), remaining_bytes)  # type: ignore[reportArgumentType, unused-ignore]

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        # feature set followed by 10 sshorts
        logging.debug("MuniCurve.length_in_bytes.self: {}".format(str(self)))
        totlen = len(self.features)
        sshort_size = array("h").itemsize
        totlen += sshort_size * self.NUM_YIELDS
        return totlen


class MuniCurvesMessageFactory:
    """Provides the framing and repeating groups to the protocol, as well
    as being able to read same from the wire.

    Format:
    SOFH 6 bytes.

    NOTE: can only really use this factory when serializing, because then you already
    know what you want to generate. When reading/deserializing you will be reading
    the message structure independently
    """

    template_id = SplineMuniFixType.Curve.value
    schema_id = SPLINE_MUNI_DATA_SCHEMA_ID
    version = SPLINE_MUNI_DATA_SCHEMA_VERSION

    # the date and time corresponding with a particular set of curves. For set:
    #
    curve_datetime: SplineDateTime
    NUM_BYTES: int = SplineDateTime.NUM_BYTES
    # for group(additional_byte_restrict=sizeof(datetime)==8)

    curve_shape = (30, 2)
    curve_schema: OrderedDict = OrderedDict(
        [("tenor", pl.Float64), ("value", pl.Float64)]
    )

    def __init__(self):
        pass

    @classmethod
    def serialize_from_dataframes(
        cls, dt_tm: dt.datetime, feature_sets: List[str], dfs: List[pl.DataFrame]
    ) -> Generator[bytes, None, None]:
        """Given a date time (must be at the proper interval already)
        of YYYYMMDD HH:MM representing curves/YYYY/MM/DD/HH/MM and
        two parallel lists, first the feature sets as strings
        ex: ["aaa_go_5_lrg_lqd", "b_rev_4_med_ilqd"]
        and then their corresponding dataframes.
        ex [pl.DataFrame(), pl.DataFrame()] where the dataframes are as loaded via pl.read_parquet()
        this will convert into a binary representation including:
        SOFH
        MessageHeader
        datetime
        simplegroupencoding
          size of one
          number of items in group
          consecutive MuniCurve that will fit into an MTU(best estimate)
        Output is bytes() that may contain 1 or more SOFH to end of message sets of bytes,
        depending on size of incoming data.
        """
        logging.debug(
            "serialize_from_dataframes got:\ndt_tm:{}\nfeature_sets: {}\ndfs: {}".format(
                dt_tm, feature_sets, dfs
            )
        )
        spline_dttm = SplineDateTime(dt_tm)
        # ok, we know what a curve should look like
        # first verfiy that all the frames look right.
        list_of_curves: List[MuniCurve] = []
        for fs_str, df in zip(feature_sets, dfs):
            shape = df.shape
            if shape != cls.curve_shape:
                raise ValueError(
                    "curve dataframes must have 2 columns and 30 rows. Got: {}".format(
                        df.head(30)
                    )
                )
            elif df.schema != cls.curve_schema:
                raise ValueError(
                    "MuniCurve got wrong schema for dataframe. Expected: {} Got: {}".format(
                        cls.curve_schema, df.schema
                    )
                )
            # then we're relatively sure it's a curve.
            fs = cast(FeatureSet, FeatureSet.from_str(fs_str))
            logging.debug("FeatureSet.from_str(): {}".format(fs))
            list_of_curves.append(MuniCurve(fs, df.get_column("value").to_list()))
        # have our curves, now we need our header
        # NOTE: We don't know the full length yet, adding SOFH as a placeholder
        # for count.
        head_buffer = BytesIO()
        head_buffer.write(Sofh().serialize())
        # message header includes the root block size of the message
        # which here will just be the datetime
        curve_mh_bytes = SplineMessageHeader(
            block_length=cls.NUM_BYTES,
            template_id=cls.template_id,
            schema_id=SPLINE_MUNI_DATA_SCHEMA_ID,
            version=SPLINE_MUNI_DATA_SCHEMA_VERSION,
            num_groups=1,
        ).serialize()
        head_buffer.write(curve_mh_bytes)
        # we'll want to align the next block to 4
        # mh_pad = align_by(message_head_length, 4)
        # mh_pad = align_by(message_head_length, 4)
        # message_head_length += mh_pad
        mh_pad = align_by(0, 4)
        head_buffer.write(mh_pad * b"\x00")

        # now we're byte aligned, we need to write our message body root
        # which for this is only our date
        dttm_ser = spline_dttm.serialize()
        head_buffer.write(dttm_ser)
        # should be 0
        after_mh_pad = align_by(0, 4)
        head_buffer.write(after_mh_pad * b"\x00")

        # dttm_ser is only 4 bytes, so don't need to worry about alignment here

        # how many muni curves can we fit in an mtu and align to 4 bytes?
        max_curves_per_message = GroupSizeEncodingSimple.max_elements_allowed(
            single_element=MuniCurve,
            additional_byte_restrict=head_buffer.tell(),
            alignment_by=4,
        )
        logging.debug(
            "MuniCurveFactory suggested max curves per message: {}".format(
                max_curves_per_message
            )
        )

        # list_of_curve_groups: List[GroupSizeEncodingSimple] = []
        curr_pos = 0
        max_pos = len(list_of_curves)
        while curr_pos < max_pos:
            buffer = BytesIO()
            buf_start_pos = buffer.tell()
            next_pos = min(curr_pos + max_curves_per_message, max_pos)
            logging.debug(
                "Loop: curr_pos: {} next_pos: {} max_pos: {}".format(
                    curr_pos, next_pos, max_pos
                )
            )
            grp_o_curves = GroupSizeEncodingSimple(
                data=list_of_curves[curr_pos:next_pos],
                additional_byte_restrict=head_buffer.tell(),
                allow_larger_than_mtu=False,
                align_to=4,
            )
            # write our header stuff
            head_buffer.seek(0, 0)
            buffer.write(head_buffer.getbuffer())
            buffer.write(grp_o_curves.serialize())
            buf_end_pos = buffer.tell()
            sofh_len = buf_end_pos - buf_start_pos
            # so, now we need to overwrite the sofh we used as a spacer
            # seek -sofh_len bytes from the end of the buffer
            pos = buffer.seek(-sofh_len, 2)
            if pos != buf_start_pos:
                raise IOError(
                    "Tried to write SOFH but got unexpected length. Buf.start_pos={} buf_pos_after_seek={}".format(
                        buf_start_pos, pos
                    )
                )
            sofh = Sofh(size=sofh_len, size_includes_self=True)
            buffer.write(sofh.serialize())
            # then go back to end
            buffer.seek(0, 2)
            curr_pos = next_pos
            yield buffer.getvalue()

    @classmethod
    def deserialize_to_dataframes_no_header(
        cls, data: bytes
    ) -> Tuple[dt.datetime, List[str], List[pl.DataFrame], bytes]:
        """Takes a byte buffer  at position just before a MuniCurveMessage
        and returns data from just that one message.
        Does not have access to header so cannot verify that it is the correct message factory,
        if padding was used before it, or if there is padding after it.
        As such, it leaves the buffer at the end of reading all curves.o
        Because the spec is written as part of this code, we know that we are
        using 4 byte alignment. This means that are start pos (0) is a 4 byte
        aligned location, because it's per spec. This also means that we know
        that each curve starts at 4 byte alignment, and should then end at
        either the end of the buffer (if message based protocol), or with
        any necessary padding to bring that one curve to a 4 byte alignment
        (at the time of writing MuniCurve is 68 bytes and needs no padding).

        returns (YYYYMMDDHHMM timestamp, list("aaa_go_5_lrg_lqd", list(pl.DataFrame), bytes_remaining))
        """
        # sofh has already been read
        # message header has already been read, and determined to be a curve message
        # next comes our date.
        (sdttm, remaining_data) = SplineDateTime.deserialize(data)
        # then we have a group of things.
        (muni_curves, remaining_data) = GroupSizeEncodingSimple.deserialize(
            remaining_data, group_type=MuniCurve, align_to=4
        )
        list_o_features: List[str] = []
        list_o_df: List[pl.DataFrame] = []

        for mc in muni_curves:
            list_o_features.append(str(mc.features))
            values = mc.values_as_float()
            df = pl.DataFrame(
                {
                    "tenor": [float(x) for x in range(1, MuniCurve.NUM_YIELDS + 1, 1)],
                    "value": values,
                }
            )
            list_o_df.append(df)
        if len(remaining_data) > 0:
            # should we eat padding?
            pad = align_by(MuniCurve.struct_size_bytes(), 4)
            logging.debug("reading {} pad bytes from remaining_data".format(pad))
            remaining_data = remaining_data[pad:]

        return (sdttm.to_datetime(), list_o_features, list_o_df, remaining_data)
