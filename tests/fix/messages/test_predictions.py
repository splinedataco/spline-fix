from typing import cast, List
from copy import copy
from io import BytesIO
import pytest  # pyright: ignore
import logging
import polars as pl
import datetime as dt
import struct
import polars.testing as pt
from dateutil.relativedelta import relativedelta
from pathlib import Path
from joshua.fix.types import SplineDateTime
from joshua.fix.messages.predictions import (
    MuniYieldPrediction,
    MuniYieldPredictionMessageFactory,
    decimal_to_float,
    par_traded_bucket_float_to_decimal,
)
from joshua.fix.messages import SplineMessageHeader
from joshua.fix.sofh import Sofh
from joshua.fix.common import align_by


"""CAUTION/NOTE: at time of implementation there were only yield calculations in the predictions
files but anticipate that there will be 3 more columns, bid, mid, and ask_price, which based on analysis
should be able to be represented with 4 byte fixed decimals with exp -3.
To account for this in testing, the test data has been modified in the following way.

```ipython
# if bid_yield is divisible by 2, copy to bid_price,
# if mid_yield is divisible by 3 copy to mid_price,
# if ask_yield is divisible by 5 copy to ask_price.
# else None.
[ins] In [186]: short_pred_lf.with_columns([pl.when(pl.col("bid_yield").cast(pl.Int64).mod(pl.lit(2)).eq(pl.lit(0))).then(pl.col("bid_yield")).otherwise(pl.lit(None)).cast(pl.Float64).alias("bid_price"),pl.when(pl.col("mid_yield").cast(pl.Int64).mod(pl.lit(3)).eq(pl.lit(0))).then(pl.col("mid_yield")).otherwise(pl.lit(None)).cast(pl.Float64).alias("mid_price"), pl.when(pl.col("ask_yield").cast(pl.Int64).mod(pl.lit(5)).eq(pl.lit(0))).then(pl.col("ask_yield")).otherwise(pl.li
           ...: t(None)).cast(pl.Float64).alias("ask_price")]).collect().write_parquet("integration_test_data/combined/predictions/2024/02/20/17/00/predictions.parquet")
```
"""


def initialize() -> None:
    # logging.getLogger().setLevel(logging.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def test_par_traded_bucket_float_to_decimal() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_par_traded_bucket_float_to_decimal -o log_cli=true
    """
    questions = [
        (100_000, 8, 8),
        (500_000, 8, 8),
        (1_000_000, 8, 8),
        (10_000, 8, 8),
    ]
    answers = [
        (1, 5),
        (5, 5),
        (1, 6),
        (1, 4),
    ]

    for (v, mb, eb), a in zip(questions, answers):
        tst_ans = par_traded_bucket_float_to_decimal(v, mb, eb)
        assert tst_ans == a


def test_par_traded_bucket_float_to_decimal_fail() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_par_traded_bucket_float_to_decimal_fail -o log_cli=true
    """
    questions = [
        (100_000, 8, 2),
        (500_000, 2, 8),
        (1_000_000, 4, 2),
        (10_000, 8, 1),
        (10_000.0, 8, 1),
        (10_000.1, 8, 1),
    ]

    answers = [
        "exponent of 5 is too large to represent with 2 bits. mantissa is 1",
        "mantissa of 5 is too large to represent with 2 bits.",
        "exponent of 6 is too large to represent with 2 bits. mantissa is 1",
        "exponent of 4 is too large to represent with 1 bits. mantissa is 1",
        "exponent of 4 is too large to represent with 1 bits. mantissa is 1",
        "par_traded_bucket_float_to_decimal only handles integers or integers as float. Got: 10000.1",
    ]

    for (v, mb, eb), a in zip(questions, answers):
        logging.debug(f"(v:{v} mb:{mb} eb: {eb}), a:'{a}'")
        with pytest.raises(ValueError) as exinfo:
            res = par_traded_bucket_float_to_decimal(v, mb, eb)
            logging.error(f"Shouldn't have gotten an answer: {res}")
        assert str(exinfo.value) == a


def test_decimal_to_float() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_decimal_to_float -o log_cli=true
    """
    q_and_a = [
        (100_000, 1, 5),
        (500_000, 5, 5),
        (1_000_000, 1, 6),
        (10_000, 1, 4),
    ]

    for v, man, exp in q_and_a:
        res = decimal_to_float(man, exp)
        assert res == v


def test_muniyield_prediction_equals() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_prediction_equals -o log_cli=true
    """
    my0 = MuniYieldPrediction(
        cusip="012345678",
        par_traded_bucket=150_000,
        bid_yield=31.557,
        mid_yield=31.677,
        ask_yield=32.123,
        bid_price=31.557,
        mid_price=31.677,
        ask_price=32.123,
    )

    assert my0 == copy(my0)
    my1 = copy(my0)
    my1.cusip = "876543210"
    assert my0 != my1
    my1 = copy(my0)
    my1.par_traded_bucket = 500_000
    assert my0 != my1
    my1 = copy(my0)
    my1.bid_yield = -23.123
    assert my0 != my1
    my1 = copy(my0)
    my1.mid_yield = -23.123
    assert my0 != my1
    my1 = copy(my0)
    my1.ask_yield = -23.123
    assert my0 != my1

    my1 = copy(my0)
    my1.bid_price = -23.123
    assert my0 != my1
    my1 = copy(my0)
    my1.mid_price = -23.123
    assert my0 != my1
    my1 = copy(my0)
    my1.ask_price = -23.123
    assert my0 != my1

    my1 = copy(my0)
    my1.bid_price = None
    assert my0 != my1
    assert my1 == my1
    my1 = copy(my0)
    my1.mid_price = None
    assert my0 != my1
    assert my1 == my1
    my1 = copy(my0)
    my1.ask_price = None
    assert my0 != my1
    assert my1 == my1


def test_muniyield_prediction_str() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_prediction_str -o log_cli=true
    """
    my0 = MuniYieldPrediction(
        cusip="012345678",
        par_traded_bucket=150_000,
        bid_yield=31.557,
        mid_yield=31.677,
        ask_yield=32.123,
    )

    my0_str = str(my0)
    logging.debug(f"my0: {my0}")
    assert (
        my0_str
        == """MuniYieldPrediction:   cusip: 012345678   par_traded_bucket: 150000.0   bid_yield: 31.557   mid_yield: 31.677   ask_yield: 32.123   $bitfield: 0b0   bid_price: None   mid_price: None   ask_price: None   padding: 2\n"""
    )

    my0 = MuniYieldPrediction(
        cusip="012345678",
        par_traded_bucket=150_000,
        bid_yield=31.557,
        mid_yield=31.677,
        ask_yield=32.123,
        bid_price=31.557,
        mid_price=None,
        ask_price=-32.123,
    )

    my0_str = str(my0)
    logging.debug(f"my0: {my0}")
    assert (
        my0_str
        == """MuniYieldPrediction:   cusip: 012345678   par_traded_bucket: 150000.0   bid_yield: 31.557   mid_yield: 31.677   ask_yield: 32.123   $bitfield: 0b1010   bid_price: 31.557   mid_price: None   ask_price: -32.123   padding: 2\n"""
    )


def test_muniyield_prediction_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_prediction_serialize -o log_cli=true
    """
    cusip = "012345678"
    par_traded_bucket = 150_000
    bid_yield = 31.557
    mid_yield = -31.677
    ask_yield = 32.123

    my0 = MuniYieldPrediction(
        cusip=cusip,
        par_traded_bucket=par_traded_bucket,
        bid_yield=bid_yield,
        mid_yield=mid_yield,
        ask_yield=ask_yield,
    )

    bb = my0.serialize()
    logging.debug(
        "serialized len = {} my0.length_in_bytes{}".format(
            len(bb), my0.length_in_bytes()
        )
    )
    assert len(bb) == my0.length_in_bytes()
    # read out string with padding
    (cusip_d,) = struct.unpack("<9s", bb[0:9])
    assert cusip_d == cusip.encode()
    (price_bitfield_d,) = struct.unpack("<B", bb[9:10])
    assert price_bitfield_d == my0._price_bitfield
    # par_bucket
    (mantissa,) = struct.unpack("<b", bb[10:11])
    assert mantissa == 15
    (exp,) = struct.unpack("<b", bb[11:12])
    assert exp == 4
    ptb_d = decimal_to_float(mantissa=mantissa, exp=exp)
    assert ptb_d == par_traded_bucket
    (bid_yield_d,) = struct.unpack("<h", bb[12:14])
    assert bid_yield_d == my0._bid_yield
    (mid_yield_d,) = struct.unpack("<h", bb[14:16])
    assert mid_yield_d == my0._mid_yield
    (ask_yield_d,) = struct.unpack("<h", bb[16:18])
    assert ask_yield_d == my0._ask_yield
    (bid_price_d,) = struct.unpack("<i", bb[18:22])
    assert bid_price_d == 0  # my0._bid_price
    (mid_price_d,) = struct.unpack("<i", bb[22:26])
    assert mid_price_d == 0  # my0._mid_price
    (ask_price_d,) = struct.unpack("<i", bb[26:30])
    assert ask_price_d == 0  # my0._ask_price

    # bb.padding  bytes of padding to bring to 4 byte alignment
    assert len(bb) == 30 + my0.padding


def test_muniyield_prediction_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_prediction_serialize_deserialize -o log_cli=true
    """
    cusip = "012345678"
    par_traded_bucket = 150_000
    bid_yield = 31.557
    mid_yield = -31.677
    ask_yield = 32.123

    my0 = MuniYieldPrediction(
        cusip=cusip,
        par_traded_bucket=par_traded_bucket,
        bid_yield=bid_yield,
        mid_yield=mid_yield,
        ask_yield=ask_yield,
    )

    bb = my0.serialize()
    (my_deser, remaining_bytes) = MuniYieldPrediction.deserialize(bb)
    assert remaining_bytes == bytes()
    logging.debug(f"my0: {my0}")
    logging.debug(f"my_deser: {my_deser}")
    assert my_deser == my0


def test_muniyield_prediction_message_factory_serialize_from_dataframe(
    integration_test_data_dir,
) -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_predictin_message_factory_serialize_from_dataframe -o log_cli=true
    """
    itdd = Path(integration_test_data_dir)
    """
    preds_20230511700_path = (
        itdd
        / "combined"
        / "predictions"
        / "2024"
        / "02"
        / "20"
        / "17"
        / "00"
        / "predictions.parquet"
    )
    """
    preds_20230511700_path = (
        itdd / "short" / "combined" / "predictions" / "predictions.parquet"
    )
    df = pl.read_parquet(preds_20230511700_path)
    # ggroup_lengths = [70, 50]
    group_lengths = [44, 44, 32]

    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2024, 2, 20, 17, 00)
    bs = BytesIO()
    for bb in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
        dt_tm=dt_tm, df=df
    ):
        # we want it to fit in a mtu
        assert len(bb) < 1500
        (sofh, remaining_bytes) = Sofh.deserialize(bb)
        logging.debug("sofh read from yield: {!r}".format(sofh))
        bs.write(bb)
    # logging.debug("{!r}".format(bs.hex(":")))
    logging.debug("len(bs.getvalue())={}".format(len(bs.getvalue())))

    # group_length_ans = [20, 4]
    # knowing what the layout should be, lets test different parts to verify

    more_bytes = True
    # go to beginning of big buffer
    bs.seek(0, 0)
    sofh_len = Sofh.NUM_BYTES
    sofh_buf = bs.read(sofh_len)
    (sofh, remaining_bytes) = Sofh.deserialize(sofh_buf)
    assert remaining_bytes == bytes()
    next_read_len = sofh.message_body_len()
    remaining_bytes = bs.read(next_read_len)
    msg_bytes_consumed = sofh_len
    total_predictions = 0

    msg_bodies = 0
    # rem_grp_len_ans = group_length_ans
    while more_bytes is True:
        # already have read sofh if here, remember to set msg_bytes_consumed = sofh.len
        # at bottom of loop
        logging.debug("remaining_bytes: {!r}".format(remaining_bytes.hex(":")))
        # lets check the datetime
        # we need to consume the message header first
        (smh, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
        smh = cast(SplineMessageHeader, smh)
        msg_bytes_consumed += SplineMessageHeader.struct_size_bytes()
        # if not at 4 byte alignment, do so now.
        pad_bytes = align_by(msg_bytes_consumed, 4)
        assert smh.template_id == 2
        assert smh.schema_id == 42
        assert smh.version == 0
        assert smh.num_groups == 1
        padding = b"\x00" * pad_bytes
        assert remaining_bytes[: len(padding)] == padding
        remaining_bytes = remaining_bytes[len(padding) :]
        msg_bytes_consumed += pad_bytes
        logging.debug("{}".format(smh))

        # next should be a datetimestamp, the body of the predictions message
        (spline_dttm, remaining_bytes) = SplineDateTime.deserialize(remaining_bytes)
        msg_bytes_consumed += SplineDateTime.struct_size_bytes()
        logging.debug("datetime: {}".format(spline_dttm))
        assert spline_dttm.to_datetime() == dt_tm + relativedelta(
            second=0, microsecond=0
        )
        dt_tm_pad = align_by(SplineDateTime.struct_size_bytes(), 4)
        padding = b"\x00" * dt_tm_pad
        assert remaining_bytes[: len(padding)] == padding
        remaining_bytes = remaining_bytes[len(padding) :]
        msg_bytes_consumed += dt_tm_pad

        # now should come the group
        (size_of_one,) = struct.unpack("<H", remaining_bytes[:2])
        # 20 because it's 18 and 4 byte aligned.
        assert size_of_one == 32
        (num_in_grp,) = struct.unpack("<H", remaining_bytes[2:4])
        logging.debug(f"num_in_group={num_in_grp} size_of_one={size_of_one}")

        remaining_bytes = remaining_bytes[4:]
        msg_bytes_consumed += 4

        assert num_in_grp == group_lengths[0]
        group_lengths = group_lengths[1:]
        total_predictions += num_in_grp

        # now to try to get a prediction from it
        (sp, remaining_bytes) = MuniYieldPrediction.deserialize(remaining_bytes)
        msg_bytes_consumed += MuniYieldPrediction.struct_size_bytes()
        logging.debug("spline prediction:\n{}".format(sp))
        my_pad = align_by(MuniYieldPrediction.struct_size_bytes(), 4)
        padding = b"\x00" * my_pad
        assert remaining_bytes[: len(padding)] == padding
        remaining_bytes = remaining_bytes[len(padding) :]
        msg_bytes_consumed += my_pad
        # now to try to get a prediction from it
        (sp, remaining_bytes) = MuniYieldPrediction.deserialize(remaining_bytes)
        msg_bytes_consumed += MuniYieldPrediction.struct_size_bytes()
        logging.debug("spline prediction:\n{}".format(sp))
        my_pad = align_by(MuniYieldPrediction.struct_size_bytes(), 4)
        padding = b"\x00" * my_pad
        assert remaining_bytes[: len(padding)] == padding
        remaining_bytes = remaining_bytes[len(padding) :]
        msg_bytes_consumed += my_pad
        # skip to the next message
        remaining_bytes = remaining_bytes[sofh.message_len() - msg_bytes_consumed :]
        # if the length is correct, the data immediately following should be SOFH
        # but, we byte aligned it to 4
        pad = align_by(sofh.message_len(), 4)
        remaining_bytes = remaining_bytes[pad:]
        msg_bytes_consumed = 0
        sofh_buf = bs.read(sofh_len)
        logging.debug("sofh_buf={!r}".format(sofh_buf))
        msg_bodies += 1
        if sofh_buf == bytes():
            break
        (sofh, remaining_bytes) = Sofh.deserialize(sofh_buf)
        assert remaining_bytes == bytes()
        next_read_len = sofh.message_body_len()
        remaining_bytes = bs.read(next_read_len)
        msg_bytes_consumed = sofh_len

        if remaining_bytes == bytes():
            more_bytes = False
        else:
            assert len(remaining_bytes) == next_read_len

    assert total_predictions == 120
    assert msg_bodies == 3


def test_muniyield_prediction_factory_deserialize_no_header(
    integration_test_data_dir,
) -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_muniyield_prediction_factory_deserialize_no_header -o log_cli=true
    """
    initialize()
    itdd = Path(integration_test_data_dir)
    preds_20230511700_path = (
        itdd / "short" / "combined" / "predictions" / "predictions.parquet"
    )
    df = pl.read_parquet(preds_20230511700_path)

    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2023, 5, 11, 17, 00)
    bs = BytesIO()
    for bb in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
        dt_tm=dt_tm, df=df
    ):
        bs.write(bb)

    group_length_ans: List[int] = [44, 44, 32]
    # knowing what the layout should be, lets test different parts to verify

    dataframes: List[pl.DataFrame] = []
    bs.seek(0, 0)
    logging.debug("len(byte_stream)={}".format(len(bs.getvalue())))
    remaining_bytes = bs.read(Sofh.struct_size_bytes())
    logging.debug("sofh bytes: {!r}".format(remaining_bytes.hex(":")))
    (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
    sofh = cast(Sofh, sofh)
    logging.debug("after sofh bytes: {!r}".format(remaining_bytes.hex(":")))
    remaining_bytes += bs.read(sofh.message_body_len())
    logging.debug("msg body len read bytes: {!r}".format(remaining_bytes.hex(":")))
    msg_bytes_consumed = Sofh.struct_size_bytes()
    # then immediately after follows the message header

    loop_count = 0
    while True:
        logging.debug("loop_count={}".format(loop_count))
        loop_count += 1
        logging.debug("len(remaining_bytes): {}".format(len(remaining_bytes)))
        msg_bytes_consumed = 0
        # first, get the sofh

        (smh, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
        smh = cast(SplineMessageHeader, smh)
        msg_bytes_consumed += SplineMessageHeader.struct_size_bytes()
        logging.debug("smh: {}".format(smh))
        logging.debug(
            "remaining_bytes after header: {!r}".format(remaining_bytes.hex(":"))
        )
        """
        # if not at 4 byte alignment, do so now.
        pad_bytes = align_by(msg_bytes_consumed, 4)
        assert remaining_bytes[:pad_bytes] == b"\x00" * pad_bytes
        msg_bytes_consumed += pad_bytes
        """
        # then we know the msgid
        assert smh.template_id == 2
        assert smh.schema_id == 42
        assert smh.version == 0
        # great, the message type we knew was there. Deserialize now.
        (dttm_de, df_de, remaining_bytes) = (
            MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
                remaining_bytes
            )
        )
        logging.debug("datetime: {}".format(dttm_de))
        logging.debug("dataframe:\n{}".format(df_de))
        dataframes.append(df_de)
        logging.debug("len(dataframes)={}".format(len(df_de)))
        assert len(df_de) == group_length_ans[0]
        group_length_ans = group_length_ans[1:]
        logging.debug("remaining_byte length: {}".format(len(remaining_bytes)))
        assert dttm_de == dt_tm
        remaining_bytes = bs.read(Sofh.struct_size_bytes())
        if remaining_bytes == bytes():
            break
        (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
        remaining_bytes += bs.read(sofh.message_body_len())
        logging.debug(
            "len(remaining_bytes) after sofh: {}".format(len(remaining_bytes))
        )
        msg_bytes_consumed = Sofh.struct_size_bytes()

    bigframe = pl.concat(dataframes).sort(by=["cusip", "par_traded_bucket"])
    logging.debug("all rows: {}".format(bigframe))
    logging.debug("original_df: {}".format(df))
    df_mod = (
        df.lazy()
        .with_columns(
            [
                pl.col("bid_yield").mul(1000).cast(pl.Int32).truediv(1000),
                pl.col("mid_yield").mul(1000).cast(pl.Int32).truediv(1000),
                pl.col("ask_yield").mul(1000).cast(pl.Int32).truediv(1000),
                pl.col("bid_price").mul(1000).cast(pl.Int32).truediv(1000),
                pl.col("mid_price").mul(1000).cast(pl.Int32).truediv(1000),
                pl.col("ask_price").mul(1000).cast(pl.Int32).truediv(1000),
            ]
        )
        .collect()
    )
    logging.debug("original_mod_df: {}".format(df_mod))
    pt.assert_frame_equal(
        left=bigframe,
        right=df_mod,
        check_row_order=True,
        check_column_order=True,
        check_dtype=True,
        check_exact=False,
        rtol=0.001,
        atol=0.001,
    )
