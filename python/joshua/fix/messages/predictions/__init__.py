try:
    from typing import Self  # type: ignore[reportAttributeAccessIssue, unused-ignore]
except ImportError:
    from typing_extensions import Self
from typing import Any, Generator, List, Optional, Tuple
from io import BytesIO, StringIO
import struct
import logging
import datetime as dt
import polars as pl
from collections import OrderedDict
from joshua.fix.types import GroupSizeEncodingSimple, SplineDateTime
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

"""Prediction messages are sent 1x per interval and consist of the following information.
Per-interval:
datetime: YYYY/MM/DD/HH/MM rounded to the interval. This directory will contain 1 file
"predictions.parquet" which will contain predictions for all cusips in the base.parquet
file. This means that upon read, we know how many results we intend to send, and we know
if the interval is finished.

And the data section contains the following 5 fields.
[ins] In [119]: predictions_lf.collect().glimpse()
Rows: 3167520
Columns: 5
$ cusip             <str> '000369EV6', '000369EV6', '000369EV6', '00036PAD4', '00036PAD4', '00036PAD4', '00036TAN4', '00036TAN4', '00036TAN4', '00036TAS3'
$ par_traded_bucket <f64> 100000.0, 500000.0, 1000000.0, 100000.0, 500000.0, 1000000.0, 100000.0, 500000.0, 1000000.0, 100000.0
$ bid_yield         <f64> 4.351730612754822, 4.110700038909912, 3.998961476325989, 3.6696701087951666, 3.4986720123291017, 3.4283912220001223, 4.268551854133606, 4.075606254577637, 4.008346704483032, 3.866240927696228
$ mid_yield         <f64> 4.20173051738739, 3.9936504342556, 3.8819117822647096, 3.600741688251496, 3.45244241142273, 3.382161829710007, 3.9702718534469605, 3.819665549039841, 3.7664068021774293, 3.7112951369285585
$ ask_yield         <f64> 4.051730422019959, 3.876600829601288, 3.7648620882034303, 3.531813267707825, 3.406212810516358, 3.3359324374198915, 3.671991852760315, 3.563724843502045, 3.5244668998718263, 3.556349346160889

```python
[nav] In [22]: predictions_lf.select([pl.col("par_traded_bucket").hist(bins=pl.col("par_traded_bucket").unique(), include_category=True)]).collect()
Out[22]:
shape: (4, 1)
┌──────────────────────────────────┐
│ par_traded_bucket                │
│ ---                              │
│ struct[2]                        │
╞══════════════════════════════════╡
│ {"(-inf, 100000.0]",1055840}     │
│ {"(100000.0, 500000.0]",1055840} │
│ {"(500000.0, 1e6]",1055840}      │
│ {"(1e6, inf]",0}                 │
└──────────────────────────────────┘
```

Based on the frequency table above, 1 byte for signed mantissa +/-128 with exp of 1 bytes gives ability)
to represent +/- 128 * 0.0000001 to +/- 128 * 10000000.
100000.0 == mantissa = 10 : exp=4, or mantissa = 1 : exp=5, etc
500000.0 == mantissa = 50 : exp=4, or mantissa = 5 : exp=5, etc
"""


def par_traded_bucket_float_to_decimal(
    value: float, mantissa_size: int = 8, exp_max_bits: int = 8
) -> Tuple[int, int]:
    """Used to convert the par_traded_bucket from a float into
    a decimal representation with a mantissa and exponent.

    Assumes:
    value.is_integer() is True
    value > 0
    significant digits are no larger than 128

    Only handles integer floats.

    example: value=1230000, mantissa_size = i8, exp_max_bits=8
    mantissa = 123
    if abs(mantissa) > 2**(8-1):
       raise Error
    exp = 4
    if abs(exp) > 2**(8-1)
        raise Error
    return (mantissa, exp)
    """
    # NOTE: py3.10 doesn't have int.is_integer, but 3.12 does
    if not isinstance(value, int) and (
        isinstance(value, float) and value.is_integer() is False
    ):
        raise ValueError(
            f"par_traded_bucket_float_to_decimal only handles integers or integers as float. Got: {value}"
        )
    mantissa = int(value)
    if mantissa == 0:
        return (0, 0)
    exp = 0
    rem = 0
    while True:
        rem = mantissa % 10
        if rem != 0:
            break
        exp += 1
        mantissa //= 10

    if mantissa.bit_length() > mantissa_size:
        raise ValueError(
            f"mantissa of {mantissa} is too large to represent with {mantissa_size} bits."
        )
    if exp.bit_length() > exp_max_bits:
        raise ValueError(
            f"exponent of {exp} is too large to represent with {exp_max_bits} bits. mantissa is {mantissa}"
        )

    return (mantissa, exp)


def decimal_to_float(mantissa: int, exp: int) -> float:
    """Used to convert from a decimal representation
    back to a float.

    """
    par_traded = float(mantissa) * 10**exp

    return par_traded


class MuniYieldPrediction(SplineFixType):
    """
    cusip
    par_traded_bucket
    bid_yield
    mid_yield
    ask_yield
    bitfield(bma0 prices)
    Optional[bid_price]
    Optional[mid_price]
    Optional[ask_price]
    """

    # cusip_len bytes
    _cusip: str  # Fixed size
    _cusip_len = 9

    # yields are going to use the default fixed length decimal for yields of -3
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    _bid_yield: int
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    _mid_yield: int
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    _ask_yield: int
    _par_traded_bucket_sizes = (8, 8)
    # 1 byte for mantissa, 1 byte for exp = 2 bytes
    # align 2
    _par_traded_bucket: Tuple[int, int]
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    # if Some() then bitfield 3 is True
    _bid_price: Optional[int]
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    # if Some() then bitfield 2 is True
    _mid_price: Optional[int]
    # 2 bytes for mantissa, exp is fixed, so 0 size
    # align: 2
    # if Some() then bitfield 1 is true
    _ask_price: Optional[int]
    _price_bitfield: int
    # 18 bytes
    # 9 cusip + 2 par + 2 by, + 2 my, + 2 ay
    #  + 1 bitfield + 4 bp + 4 mp + 4 ap
    # total = 30 % 4 = 2, 4-2 = 2
    NUM_BYTES = 9 + 2 + 2 + 2 + 2 + 13
    _pad = 2

    @classmethod
    def static_padding(cls, align_to: int = 4) -> int:
        pad = align_by(cls.NUM_BYTES, align_to)
        return pad

    @property
    def padding(self) -> int:
        return self._pad

    @padding.setter
    def padding(self, align_to: int = 4) -> None:
        self._pad = align_by(self.NUM_BYTES, align_to)
        logging.debug(
            "_pad={} align_to: {} NUM_BYTES={}".format(
                self._pad, align_to, self.NUM_BYTES
            )
        )

    @property
    def cusip(self) -> str:
        return self._cusip

    @cusip.setter
    def cusip(self, cusip: str) -> None:
        if len(cusip) != self._cusip_len:
            raise ValueError(
                f"cusips must be {self._cusip_len} bytes long. Got: {cusip}"
            )
        self._cusip = cusip

    @property
    def bid_yield(self) -> float:
        return yield_fixed_decimal_to_float(self._bid_yield)

    @bid_yield.setter
    def bid_yield(self, bid_yield: float) -> None:
        self._bid_yield = yield_float_to_fixed_decimal(bid_yield)

    @property
    def mid_yield(self) -> float:
        return yield_fixed_decimal_to_float(self._mid_yield)

    @mid_yield.setter
    def mid_yield(self, mid_yield: float) -> None:
        self._mid_yield = yield_float_to_fixed_decimal(mid_yield)

    @property
    def ask_yield(self) -> float:
        return yield_fixed_decimal_to_float(self._ask_yield)

    @ask_yield.setter
    def ask_yield(self, ask_yield: float) -> None:
        self._ask_yield = yield_float_to_fixed_decimal(ask_yield)

    @property
    def bid_price(self) -> Optional[float]:
        if self._bid_price:
            return yield_fixed_decimal_to_float(self._bid_price)
        return None

    @bid_price.setter
    def bid_price(self, bid_price: Optional[float]) -> None:
        if isinstance(bid_price, float):
            self._bid_price = yield_float_to_fixed_decimal(bid_price, mantissa_size=32)
            self._price_bitfield |= 8
        elif bid_price is None:
            self._bid_price = None
            self._price_bitfield &= 0b0111
        else:
            raise ValueError(
                "bid_price must be float or None. Got: {}".format(bid_price)
            )

    @property
    def mid_price(self) -> Optional[float]:
        if self._mid_price:
            return yield_fixed_decimal_to_float(self._mid_price)
        return None

    @mid_price.setter
    def mid_price(self, mid_price: Optional[float]) -> None:
        if isinstance(mid_price, float):
            self._mid_price = yield_float_to_fixed_decimal(mid_price, mantissa_size=32)
            self._price_bitfield |= 4
        elif mid_price is None:
            self._mid_price = None
            self._price_bitfield &= 0b1011
        else:
            raise ValueError(
                "mid_price must be float or None. Got: {}".format(mid_price)
            )

    @property
    def ask_price(self) -> Optional[float]:
        if self._ask_price:
            return yield_fixed_decimal_to_float(self._ask_price)
        else:
            return None

    @ask_price.setter
    def ask_price(self, ask_price: Optional[float]) -> None:
        if isinstance(ask_price, float):
            self._ask_price = yield_float_to_fixed_decimal(ask_price, mantissa_size=32)
            self._price_bitfield |= 2
        elif ask_price is None:
            self._ask_price = None
            self._price_bitfield &= 0b1101
        else:
            raise ValueError(
                "ask_price must be float or None. Got: {}".format(ask_price)
            )

    @property
    def par_traded_bucket(self) -> float:
        return decimal_to_float(
            mantissa=self._par_traded_bucket[0], exp=self._par_traded_bucket[1]
        )

    @par_traded_bucket.setter
    def par_traded_bucket(self, par_traded_bucket: float) -> None:
        self._par_traded_bucket = par_traded_bucket_float_to_decimal(
            value=par_traded_bucket,
            mantissa_size=self._par_traded_bucket_sizes[0],
            exp_max_bits=self._par_traded_bucket_sizes[1],
        )

    def __init__(
        self,
        cusip: str,
        par_traded_bucket: float,
        bid_yield: float,
        mid_yield: float,
        ask_yield: float,
        bid_price: Optional[float] = None,
        mid_price: Optional[float] = None,
        ask_price: Optional[float] = None,
        align_to: int = 4,
    ):
        self._price_bitfield = 0
        self.cusip = cusip
        self.par_traded_bucket = par_traded_bucket
        self.bid_yield = bid_yield
        self.mid_yield = mid_yield
        self.ask_yield = ask_yield
        self.bid_price = bid_price
        self.mid_price = mid_price
        self.ask_price = ask_price
        self.padding = align_to

    @classmethod
    def struct_size_bytes(cls) -> int:
        return cls.NUM_BYTES + cls.static_padding(align_to=4)

    @property
    def block_length(self) -> int:
        return self.struct_size_bytes()

    def __repr__(self) -> str:
        return str(self)

    def __str__(self) -> str:
        outbuf = StringIO()
        outbuf.write("MuniYieldPrediction:")
        outbuf.write(f"   cusip: {self.cusip}")
        outbuf.write(f"   par_traded_bucket: {self.par_traded_bucket}")
        outbuf.write(f"   bid_yield: {self.bid_yield}")
        outbuf.write(f"   mid_yield: {self.mid_yield}")
        outbuf.write(f"   ask_yield: {self.ask_yield}")
        outbuf.write(f"   $bitfield: {bin(self._price_bitfield)}")
        outbuf.write(f"   bid_price: {self.bid_price}")
        outbuf.write(f"   mid_price: {self.mid_price}")
        outbuf.write(f"   ask_price: {self.ask_price}")
        outbuf.write(f"   padding: {self.padding}")
        outbuf.write("\n")

        return outbuf.getvalue()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MuniYieldPrediction):
            return NotImplemented
        return (
            self.cusip,
            self.par_traded_bucket,
            self.bid_yield,
            self.mid_yield,
            self.ask_yield,
            self._price_bitfield,
            self.bid_price,
            self.mid_price,
            self.ask_price,
        ) == (
            other.cusip,
            other.par_traded_bucket,
            other.bid_yield,
            other.mid_yield,
            other.ask_yield,
            other._price_bitfield,
            other.bid_price,
            other.mid_price,
            other.ask_price,
        )

    def serialize(self) -> bytes:
        """Turn ourselves into a little_endian array of bytes"""
        ser_ret = BytesIO()
        # cusip
        fmt_str = "<{}s".format(self._cusip_len)
        logging.debug("format_str for cusip is: {}".format(fmt_str))
        ser_ret.write(struct.pack(fmt_str, self._cusip.encode()))
        # price_bitfield
        ser_ret.write(struct.pack("<B", self._price_bitfield))
        # align next field to 2
        # nop, already aligned # cusip_bytes += b"\x00"
        # par_traded_bucket
        ser_ret.write(struct.pack("<b", self._par_traded_bucket[0]))
        ser_ret.write(struct.pack("<b", self._par_traded_bucket[1]))
        # bid_yield
        ser_ret.write(struct.pack("<h", self._bid_yield))
        # mid_yield
        ser_ret.write(struct.pack("<h", self._mid_yield))
        # ask_yield
        ser_ret.write(struct.pack("<h", self._ask_yield))
        # need different handling because may be null
        # bid_price
        bp = self._bid_price if self._bid_price is not None else 0
        ser_ret.write(struct.pack("<i", bp))
        # mid_price
        mp = self._mid_price if self._mid_price is not None else 0
        ser_ret.write(struct.pack("<i", mp))
        # ask_price
        ap = self._ask_price if self._ask_price is not None else 0
        ser_ret.write(struct.pack("<i", ap))

        # padding for alignment of x
        ser_ret.write(b"\x00" * self.padding)

        logging.debug(
            "MuniYieldPredictions.serialize return: {!r}".format(
                ser_ret.getvalue().hex(":")
            )
        )
        return ser_ret.getvalue()

    @classmethod
    def deserialize(
        cls, data: bytes, block_type: Any = None, align_to: int = 4
    ) -> tuple[Self, bytes]:
        """Neither block_type nor align_to are used"""
        logging.debug(
            "deserialize bytes in len: {} bytes: '{}'".format(len(data), data.hex())
        )
        # cusip
        fmt_str = "<{}s".format(cls._cusip_len)
        (cusip,) = struct.unpack(fmt_str, data[0 : cls._cusip_len])
        (price_bitfield,) = struct.unpack("<B", data[9:10])
        # par_traded_bucket
        (ptm,) = struct.unpack("<b", data[10:11])
        (pte,) = struct.unpack("<b", data[11:12])
        par_traded_bucket = (ptm, pte)
        # bid_yield
        (bid_yield,) = struct.unpack("<h", data[12:14])
        # mid_yield
        (mid_yield,) = struct.unpack("<h", data[14:16])
        # ask_yield
        (ask_yield,) = struct.unpack("<h", data[16:18])
        (bid_price,) = struct.unpack("<i", data[18:22])
        (mid_price,) = struct.unpack("<i", data[22:26])
        (ask_price,) = struct.unpack("<i", data[26:30])

        # can't make a default object so will put in vals we intend to overwrite
        ypred = cls(
            cusip=cusip.decode("latin-1"),
            par_traded_bucket=0,
            bid_yield=0,
            mid_yield=0,
            ask_yield=0,
        )
        ypred._par_traded_bucket = par_traded_bucket
        ypred._bid_yield = bid_yield
        ypred._mid_yield = mid_yield
        ypred._ask_yield = ask_yield
        ypred._price_bitfield = price_bitfield
        ypred._bid_price = bid_price
        ypred._mid_price = mid_price
        ypred._ask_price = ask_price

        # use NUM_BYTES and padding to consume padding in message
        return (ypred, data[cls.NUM_BYTES + cls.static_padding(align_to=4) :])  # type: ignore[reportArgumentType, unused-ignore]

    def length_in_bytes(self) -> int:
        """Returns size of self when serialized."""
        return self.NUM_BYTES + self.padding


class MuniYieldPredictionMessageFactory:
    """Provides the framing and repeating groups to the protocol, as well
    as being able to read same from the wire.

    Format:
    SOFH 6 bytes.

    NOTE: can only really use this factory when serializing, because then you already
    know what you want to generate. When reading/deserializing you will be reading
    the message structure independently
    """

    template_id = SplineMuniFixType.Prediction.value
    schema_id = SPLINE_MUNI_DATA_SCHEMA_ID
    version = SPLINE_MUNI_DATA_SCHEMA_VERSION
    # the date and time corresponding with a particular set of curves. For set:
    #
    yields_datetime: SplineDateTime
    NUM_BYTES: int = SplineDateTime.NUM_BYTES
    dt_tm_padding = align_by(numbytes=NUM_BYTES, alignment_by=4)
    NUM_BYTES += dt_tm_padding
    # for group(additional_byte_restrict=sizeof(datetime)==8)

    yield_pred_schema: OrderedDict = OrderedDict(
        [
            ("cusip", pl.String),
            ("par_traded_bucket", pl.Float64),
            ("bid_yield", pl.Float64),
            ("mid_yield", pl.Float64),
            ("ask_yield", pl.Float64),
            # NOTE: these haven't been added to the file yet
            # NOTE: After being added, these are actually Optional[pl.Float64)
            ("bid_price", pl.Float64),
            ("mid_price", pl.Float64),
            ("ask_price", pl.Float64),
        ]
    )
    yield_pred_schema_old: OrderedDict = OrderedDict(
        [
            ("cusip", pl.String),
            ("par_traded_bucket", pl.Float64),
            ("bid_yield", pl.Float64),
            ("mid_yield", pl.Float64),
            ("ask_yield", pl.Float64),
        ]
    )

    # NOTE: Shape will now be:
    # SOFH(6 bytes)
    # MsgHdr(6 bytes)
    # pad(0)
    # datetime(4 bytes)   : 16 msg totol
    # pad(0)
    # simple group
    #    blockLen(2 bytes)  blockLen is len of one group element: 24 B
    #    numInGrp(2 bytes) : 20 msg total
    # pad(0)
    # cusip(9 bytes)
    # price_bitfield(1 B) : 3210 Bits. 0 means no fields are populated, any bit populated means that
    # 8 = bid price
    # 4 = mid price
    # 2 = ask price
    # field has info
    #                     : 30 msg total
    # by(2 B)
    # my(2 B)
    # ay(2 B)              : 36 msg total
    # bp(4 B)
    # mp(4 B)
    # ap(4 B)              : 48 msg total
    # no post msg padding as divisible by 4.

    @classmethod
    def serialize_from_dataframe(
        cls, dt_tm: dt.datetime, df: pl.DataFrame
    ) -> Generator[bytes, None, None]:
        """Given a date time (must be at the proper interval already, ex: mod 5min)
        of YYYYMMDD HH:MM representing curves/YYYY/MM/DD/HH/MM and
        the dataframe of predictions, returns a byte array of multiple messages containing
        the all of the dataframe.
        this will convert into a binary representation including:
        SOFH
        MessageHeader
        datetime
        simplegroupencoding
          size of one
          number of items in group
          consecutive MuniYieldPredictions that will fit into an MTU(best estimate)
        Output is bytes() that may contain 1 or more SOFH to end of message sets of bytes,
        depending on size of incoming data.
        """
        logging.debug(
            "serialize_from_dataframes got:\ndt_tm:{}\ndf: {}".format(dt_tm, df)
        )
        # first, lets verify that the schema is what we expect
        if df.schema != cls.yield_pred_schema:
            # we need to check to see if it's the 3 "expected" missing fields
            if df.schema == cls.yield_pred_schema_old:
                df = (
                    df.lazy()
                    .with_columns(
                        [
                            pl.lit(None).cast(pl.Float64).alias("bid_price"),
                            pl.lit(None).cast(pl.Float64).alias("mid_price"),
                            pl.lit(None).cast(pl.Float64).alias("ask_price"),
                        ]
                    )
                    .collect()
                )
            else:
                raise ValueError(
                    f"dataframe does not match expected schema. Expected: {cls.yield_pred_schema} Got: {df.schema}"
                )
        spline_dttm = SplineDateTime(dt_tm)

        # have what looks like a prediction, now we need our header
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
        # message_head_length += mh_pad
        mh_pad = align_by(head_buffer.tell(), 4)
        head_buffer.write(mh_pad * b"\x00")

        # now we're byte aligned, we need to write our message body root
        # which for this is only our date
        dttm_ser = spline_dttm.serialize()
        head_buffer.write(dttm_ser)
        # dttm_ser is only 4 bytes, so don't need to worry about alignment here
        # but we'll do it for forward compatibility
        after_mh_pad = align_by(head_buffer.tell(), 4)
        head_buffer.write(after_mh_pad * b"\x00")

        # how many muni yield predictions can we fit in an mtu and align to 4 bytes?
        max_predictions_per_message = GroupSizeEncodingSimple.max_elements_allowed(
            single_element=MuniYieldPrediction,
            additional_byte_restrict=head_buffer.tell(),
            alignment_by=4,
        )
        logging.debug(
            "MuniYieldPredictionMessageFactory suggested max predictions per message: {}".format(
                max_predictions_per_message
            )
        )

        # NOTE: iter_rows on polars dataframes is inefficient because the data is stored
        # in column order, but here we are.

        # number of rows in the dataframe
        max_pos = df.shape[0]
        list_of_predictions: List[MuniYieldPrediction] = []
        num_processed = 0
        for yp in df.iter_rows(named=True):
            buffer = BytesIO()
            y = MuniYieldPrediction(
                cusip=yp["cusip"],
                par_traded_bucket=yp["par_traded_bucket"],
                bid_yield=yp["bid_yield"],
                mid_yield=yp["mid_yield"],
                ask_yield=yp["ask_yield"],
                bid_price=yp.get("bid_price"),
                mid_price=yp.get("mid_price"),
                ask_price=yp.get("ask_price"),
            )
            list_of_predictions.append(y)
            num_processed += 1
            if (
                len(list_of_predictions) < max_predictions_per_message
                and num_processed < max_pos
            ):
                continue

            # either we have enough for a batch, or we are handling the last of the
            # rows.
            buf_start_pos = buffer.tell()
            grp_o_predictions = GroupSizeEncodingSimple(
                data=list_of_predictions,
                additional_byte_restrict=head_buffer.tell(),
                allow_larger_than_mtu=False,
                align_to=4,
            )
            # write our header stuff
            head_buffer.seek(0, 0)
            buffer.write(head_buffer.getbuffer())
            buffer.write(grp_o_predictions.serialize())
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
            # empty out our list
            list_of_predictions.clear()
            yield buffer.getvalue()

    @classmethod
    def deserialize_to_dataframe_no_header(
        cls, data: bytes
    ) -> Tuple[dt.datetime, pl.DataFrame, bytes]:
        """Takes a byte buffer  at position just before a MuniYieldPredictionMessage
        and returns data from just that one message.
        Does not have access to header so cannot verify that it is the correct message factory,
        if padding was used before it, or if there is padding after it.
        As such, it leaves the buffer at the end of reading all curves.o
        Because the spec is written as part of this code, we know that we are
        using 4 byte alignment. This means that are start pos (0) is a 4 byte
        aligned location, because it's per spec. This also means that we know
        that each prediction starts at 4 byte alignment, and should then end at
        either the end of the buffer (if message based protocol), or with
        any necessary padding to bring that one curve to a 4 byte alignment
        (at the time of writing MuniYieldPrediction is 18 bytes and needs requires 2 bytes of padding).

        returns (YYYYMMDDHHMM timestamp, pl.DataFrame, bytes_remaining))
        """
        # sofh has already been read
        # message header has already been read, and determined to be a curve message
        # next comes our date.
        (sdttm, remaining_data) = SplineDateTime.deserialize(data)
        # then we have a group of things.
        (muni_predictions, remaining_data) = GroupSizeEncodingSimple.deserialize(
            remaining_data, group_type=MuniYieldPrediction, align_to=4
        )

        list_o_predictions = []

        for mp in muni_predictions:
            list_o_predictions.append(mp)

        cusips: List[str] = []
        par_bucket: List[float] = []
        bid_yield: List[float] = []
        mid_yield: List[float] = []
        ask_yield: List[float] = []
        bid_price: List[float] = []
        mid_price: List[float] = []
        ask_price: List[float] = []

        for p in list_o_predictions:
            cusips.append(p.cusip)
            par_bucket.append(p.par_traded_bucket)
            bid_yield.append(p.bid_yield)
            mid_yield.append(p.mid_yield)
            ask_yield.append(p.ask_yield)
            bid_price.append(p.bid_price)
            mid_price.append(p.mid_price)
            ask_price.append(p.ask_price)

        """
        logging.error(
            "before make yield dataframe: {}".format(dt.datetime.now().timestamp())
        )
        """
        pred_df = pl.DataFrame(
            data={
                "cusip": cusips,
                "par_traded_bucket": par_bucket,
                "bid_yield": bid_yield,
                "mid_yield": mid_yield,
                "ask_yield": ask_yield,
                "bid_price": bid_price,
                "mid_price": mid_price,
                "ask_price": ask_price,
            },
            schema={
                "cusip": pl.String,
                "par_traded_bucket": pl.Float64,
                "bid_yield": pl.Float64,
                "mid_yield": pl.Float64,
                "ask_yield": pl.Float64,
                "bid_price": pl.Float64,
                "mid_price": pl.Float64,
                "ask_price": pl.Float64,
            },
        )
        """
        logging.error(
            "after make yield dataframe: {}".format(dt.datetime.now().timestamp())
        )
        """

        return (sdttm.to_datetime(), pred_df, remaining_data)
