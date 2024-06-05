import pytest  # pyright: ignore
import logging
import regex
import polars as pl
import datetime as dt
import struct
from pathlib import Path
from joshua.fix.messages import (
    MuniPayment,
    MuniSize,
    MuniLiquidity,
    SplineMessageHeader,
)
from joshua.fix.messages.curves import (
    MuniCurve,
)
from joshua.fix.messages.featureset import FeatureSet
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.sofh import Sofh
from joshua.fix.common import align_by
from joshua.fix.types import SplineDateTime
from dateutil.relativedelta import relativedelta


def initialize() -> None:
    # logging.getLogger().setLevel(logging.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def test_serialize_featureset() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_serialize_featureset -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )

    assert "aaa_go_5_lrg_lqd" == str(aaa_go_5_lrg_lqd)
    aaa_go_5_lrg_lqd_ser = aaa_go_5_lrg_lqd.serialize()
    logging.debug(
        "{} serialized: '{!r}'".format(aaa_go_5_lrg_lqd, aaa_go_5_lrg_lqd_ser)
    )
    assert aaa_go_5_lrg_lqd_ser == b"AAA\x00\x01\x05\x03\x03"

    aa_rev_4_med_ilqd = FeatureSet(
        rating="AA",
        payment=MuniPayment.rev,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.ilqd,
    )

    assert "aa_rev_4_med_ilqd" == str(aa_rev_4_med_ilqd)
    aa_rev_4_med_ilqd_ser = aa_rev_4_med_ilqd.serialize()
    logging.debug(
        "{} serialized: '{!r}'".format(aa_rev_4_med_ilqd, aa_rev_4_med_ilqd_ser)
    )
    # NOTE: the extra padding for AAA
    assert aa_rev_4_med_ilqd_ser == b"AA\x00\x00\x02\x04\x02\x01"


def test_featureset_equality() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_featureset_equality -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )

    aa_rev_4_med_ilqd = FeatureSet(
        rating="AA",
        payment=MuniPayment.rev,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.ilqd,
    )

    assert aaa_go_5_lrg_lqd != aa_rev_4_med_ilqd
    assert aaa_go_5_lrg_lqd == aaa_go_5_lrg_lqd


def test_deserialize_featureset() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_deserialize_featureset -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )

    aaa_go_5_lrg_lqd_ser = b"AAA\x00\x01\x05\x03\x03"
    rehydrated_aaa_go_5_lrg_lqd, remaining_bytes = aaa_go_5_lrg_lqd.deserialize(
        aaa_go_5_lrg_lqd_ser
    )
    logging.debug(
        "{} deserialized from: '{!r}'".format(
            rehydrated_aaa_go_5_lrg_lqd, aaa_go_5_lrg_lqd_ser
        )
    )
    assert rehydrated_aaa_go_5_lrg_lqd == aaa_go_5_lrg_lqd
    assert bytes() == remaining_bytes

    aa_rev_4_med_ilqd = FeatureSet(
        rating="AA",
        payment=MuniPayment.rev,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.ilqd,
    )

    # NOTE: the extra padding for AA
    aa_rev_4_med_ilqd_ser = b"AA\x00\x00\x02\x04\x02\x01"
    (rehydrated_aa_rev_4_med_ilqd, remaining_bytes) = aa_rev_4_med_ilqd.deserialize(
        aa_rev_4_med_ilqd_ser
    )
    logging.debug(
        "{} deserialized from: '{!r}'".format(
            rehydrated_aa_rev_4_med_ilqd, aa_rev_4_med_ilqd_ser
        )
    )
    assert rehydrated_aa_rev_4_med_ilqd == aa_rev_4_med_ilqd
    assert bytes() == remaining_bytes

    # try 2 back to back

    two_fs = aaa_go_5_lrg_lqd_ser + aa_rev_4_med_ilqd_ser
    logging.debug("2 featuresets jammed together: '{}'".format(two_fs.hex()))

    (rehydrated_aaa_go_5_lrg_lqd, remaining_bytes) = FeatureSet.deserialize(two_fs)
    logging.debug("first featureset: {}".format(rehydrated_aaa_go_5_lrg_lqd))
    logging.debug("remaining bytes: '{}'".format(remaining_bytes.hex()))
    assert rehydrated_aaa_go_5_lrg_lqd == aaa_go_5_lrg_lqd

    (rehydrated_aa_rev_4_med_ilqd, remaining_bytes) = FeatureSet.deserialize(
        remaining_bytes
    )
    logging.debug("first featureset: {}".format(rehydrated_aaa_go_5_lrg_lqd))
    assert rehydrated_aa_rev_4_med_ilqd == aa_rev_4_med_ilqd
    assert bytes() == remaining_bytes

    # try a full 4 character rating
    bbbminus_go_4_med_lqd = FeatureSet(
        rating="BBB-",
        payment=MuniPayment.go,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.lqd,
    )

    bbbminus_go_4_med_lqd_ser = bbbminus_go_4_med_lqd.serialize()
    bbbm_g4ml_ser_man = b"BBB-\x01\x04\x02\x03"
    assert bbbminus_go_4_med_lqd_ser == bbbm_g4ml_ser_man

    # reserialize from this
    (rehydrated_bbbminus_go_4_med_lqd, _) = FeatureSet.deserialize(bbbm_g4ml_ser_man)
    assert rehydrated_bbbminus_go_4_med_lqd == bbbminus_go_4_med_lqd

    # try a 0 character rating
    with pytest.raises(ValueError):
        FeatureSet("", MuniPayment.go, 5, MuniSize.lrg, MuniLiquidity.lqd)

    bad_rating_ser = b"\x00\x00\x00\x00\x01\x04\x02\x03"
    with pytest.raises(ValueError):
        FeatureSet.deserialize(bad_rating_ser)


def test_municurve_str() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurve_str -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )
    values = [float(x) for x in range(1, 31, 1)]
    mc = MuniCurve(aaa_go_5_lrg_lqd, values)
    logging.debug("MuniCurve:\n{}".format(mc))
    ans = """aaa_go_5_lrg_lqd:
tenor|value
-----|-----
1.0  | 1.000
2.0  | 2.000
3.0  | 3.000
4.0  | 4.000
5.0  | 5.000
6.0  | 6.000
7.0  | 7.000
8.0  | 8.000
9.0  | 9.000
10.0  | 10.000
11.0  | 11.000
12.0  | 12.000
13.0  | 13.000
14.0  | 14.000
15.0  | 15.000
16.0  | 16.000
17.0  | 17.000
18.0  | 18.000
19.0  | 19.000
20.0  | 20.000
21.0  | 21.000
22.0  | 22.000
23.0  | 23.000
24.0  | 24.000
25.0  | 25.000
26.0  | 26.000
27.0  | 27.000
28.0  | 28.000
29.0  | 29.000
30.0  | 30.000
"""
    assert str(mc) == ans


def test_municurve_equals() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurve_equals -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )

    values0 = [float(x) for x in range(-1, -31, -1)]
    values1 = [float(x) for x in range(1, 31, 1)]
    mc0 = MuniCurve(aaa_go_5_lrg_lqd, values0)
    mc1 = MuniCurve(aaa_go_5_lrg_lqd, values1)

    assert mc0 == mc0
    assert mc0 != mc1
    bbbminus_go_4_med_lqd = FeatureSet(
        rating="BBB-",
        payment=MuniPayment.go,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.lqd,
    )
    mc2 = MuniCurve(bbbminus_go_4_med_lqd, values0)
    mc3 = MuniCurve(bbbminus_go_4_med_lqd, values1)
    assert mc0 != mc2
    assert mc1 != mc2
    assert mc2 != mc3
    assert mc3 != mc2
    assert mc2 == mc2
    assert mc3 == mc3


def test_municurve_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurve_serialize_deserialize -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )
    values = [float(x) for x in range(1, 31, 1)]
    mc = MuniCurve(aaa_go_5_lrg_lqd, values)
    logging.debug("MuniCurve: {}".format(mc))
    mc_ser = mc.serialize()
    logging.debug("MuniCurve.serialized: '{!r}'".format(mc_ser))

    (rehydrated_mc, remaining_bytes) = MuniCurve.deserialize(mc_ser)
    logging.debug("rehydrated_mc:\n{}".format(rehydrated_mc))
    assert rehydrated_mc == mc
    assert remaining_bytes == bytes()


def test_municurve_deserialize_multiple() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurve_deserialize_multiple -o log_cli=true
    """
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )
    bbbminus_go_4_med_lqd = FeatureSet(
        rating="BBB-",
        payment=MuniPayment.go,
        coupon=4,
        muni_size=MuniSize.med,
        liquidity=MuniLiquidity.lqd,
    )

    values0 = [float(x) for x in range(-1, -31, -1)]
    values1 = [float(x) for x in range(1, 31, 1)]
    mc0 = MuniCurve(aaa_go_5_lrg_lqd, values0)
    mc1 = MuniCurve(bbbminus_go_4_med_lqd, values1)

    # put them back to back after serializing and see if we can get 2 back
    mc0_ser = mc0.serialize()
    mc1_ser = mc1.serialize()

    bigbuf = mc0_ser + mc1_ser

    list_o_curves = []

    rem_bytes = bigbuf
    while len(rem_bytes) > 0:
        (rehydrated_mc, rem_bytes) = MuniCurve.deserialize(rem_bytes)
        list_o_curves.append(rehydrated_mc)

    assert list_o_curves[0] == mc0
    assert list_o_curves[1] == mc1
    assert rem_bytes == bytes()


def test_municurve_size() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurve_size -o log_cli=true
    """
    values0 = [
        2.631,
        2.518,
        2.403,
        2.286,
        2.265,
        3.058,
        3.095,
        3.132,
        3.17,
        -3.207,
        2.631,
        2.518,
        2.403,
        2.286,
        2.265,
        3.058,
        3.095,
        3.132,
        3.17,
        -3.207,
        2.631,
        2.518,
        2.403,
        2.286,
        2.265,
        3.058,
        3.095,
        3.132,
        3.17,
        -3.207,
    ]
    aaa_go_5_lrg_lqd = FeatureSet(
        rating="AAA",
        payment=MuniPayment.go,
        coupon=5,
        muni_size=MuniSize.lrg,
        liquidity=MuniLiquidity.lqd,
    )
    mc0 = MuniCurve(aaa_go_5_lrg_lqd, values0)
    mc0_size = len(mc0)
    # expected size.
    # size of feature Set
    # 4 bytes rating
    # 1 byte payment
    # 1 byte coupon
    # 1 byte size
    # 1 byte liquidity
    # = 8 bytes
    # plus
    # 30 curves * 2 bytes
    # = 60
    # == 68 bytes
    assert mc0_size == 68


def test_featureset_from_str() -> None:
    fs_re = regex.compile(
        r"([abc]{1,})_(go|rev)_([[:digit:]]{1,2})_(lrg|med|sml)_(lqd|ilqd)"
    )

    tst_str = ["aaa_go_5_lrg_lqd", "AAA_rev_4_med_ILQD", "b_rev_10_sml_ilqd"]
    ans = [
        FeatureSet("AAA", MuniPayment.go, 5, MuniSize.lrg, MuniLiquidity.lqd),
        FeatureSet("AAA", MuniPayment.rev, 4, MuniSize.med, MuniLiquidity.ilqd),
        FeatureSet("B", MuniPayment.rev, 10, MuniSize.sml, MuniLiquidity.ilqd),
    ]
    for ts, a in zip(tst_str, ans):
        group = fs_re.match(ts.lower())
        if group:
            groups = group.groups()
            logging.debug("str: {} groups: {}".format(ts, groups))
            fs = FeatureSet(
                groups[0].lower(),
                MuniPayment[groups[1].lower()],
                int(groups[2]),
                MuniSize[groups[3].lower()],
                MuniLiquidity[groups[4].lower()],
            )
            logging.debug("new featureset is:\n{}".format(fs))
            assert "_".join(groups) == str(fs)
            assert str(fs) == str(a)

    features = [
        "aaa_go_5_lrg_lqd",
        "aaa_go_5_med_lqd",
        "aaa_go_5_sml_lqd",
        "aaa_rev_5_lrg_lqd",
        "aaa_rev_5_med_lqd",
        "aaa_rev_5_sml_lqd",
        "a_go_5_lrg_lqd",
        "a_go_5_med_lqd",
        "a_go_5_sml_lqd",
        "a_rev_5_lrg_lqd",
        "a_rev_5_med_lqd",
        "a_rev_5_sml_lqd",
        "aa_go_5_lrg_lqd",
        "aa_go_5_med_lqd",
        "aa_go_5_sml_lqd",
        "aa_rev_5_lrg_lqd",
        "aa_rev_5_med_lqd",
        "aa_rev_5_sml_lqd",
        "bbb_go_5_lrg_lqd",
        "bbb_go_5_med_lqd",
        "bbb_go_5_sml_lqd",
        "bbb_rev_5_lrg_lqd",
        "bbb_rev_5_med_lqd",
        "bbb_rev_5_sml_lqd",
    ]

    for f in features:
        tfs = FeatureSet.from_str(f)
        logging.debug("test_feature:ans_feature {}:{}".format(tfs, f))
        assert tfs is not None
        assert str(tfs) == f

    path = "/home/user/s3/curves/2024/05/10/11/35/aaa_go_5_lrg_lqd/curve.parquet"
    fs = FeatureSet.from_str(path)
    assert fs is not None
    assert str(fs) == "aaa_go_5_lrg_lqd"


def test_municurves_message_factory_serialize_from_dataframes(  # type: ignore[no-untyped-def]
    integration_test_data_dir,
) -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurves_message_factory_serialize_from_dataframes -o log_cli=true
    """
    itdd = Path(integration_test_data_dir)
    curves_20230511700_dir = itdd / "curves" / "2023" / "05" / "11" / "17" / "00"
    features = [
        "aaa_go_5_lrg_lqd",
        "aaa_go_5_med_lqd",
        "aaa_go_5_sml_lqd",
        "aaa_rev_5_lrg_lqd",
        "aaa_rev_5_med_lqd",
        "aaa_rev_5_sml_lqd",
        "a_go_5_lrg_lqd",
        "a_go_5_med_lqd",
        "a_go_5_sml_lqd",
        "a_rev_5_lrg_lqd",
        "a_rev_5_med_lqd",
        "a_rev_5_sml_lqd",
        "aa_go_5_lrg_lqd",
        "aa_go_5_med_lqd",
        "aa_go_5_sml_lqd",
        "aa_rev_5_lrg_lqd",
        "aa_rev_5_med_lqd",
        "aa_rev_5_sml_lqd",
        "bbb_go_5_lrg_lqd",
        "bbb_go_5_med_lqd",
        "bbb_go_5_sml_lqd",
        "bbb_rev_5_lrg_lqd",
        "bbb_rev_5_med_lqd",
        "bbb_rev_5_sml_lqd",
    ]
    dfs = []

    for feat in features:
        path = curves_20230511700_dir / feat / "curve.parquet"
        logging.debug("test path is: {}".format(path))
        dfs.append(pl.read_parquet(path))

    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2023, 5, 11, 17, 00)
    curves = [
        c
        for c in MuniCurvesMessageFactory.serialize_from_dataframes(
            dt_tm=dt_tm, feature_sets=features, dfs=dfs
        )
    ]
    # logging.debug("{!r}".format(bs.hex(":")))

    group_length_ans = [20, 4]
    # knowing what the layout should be, lets test different parts to verify

    msg_bodies = 0
    rem_grp_len_ans = group_length_ans
    while len(curves) > 0:
        remaining_bytes = curves[0]
        msg_bytes_consumed = 0
        logging.debug("remaining_bytes: {!r}".format(remaining_bytes.hex(":")))
        # get the length of the first message out
        (length,) = struct.unpack("!L", remaining_bytes[0:4])
        logging.debug("message({}) length: {}".format(msg_bodies, length))
        assert length == len(remaining_bytes)
        # we want it to fit in a mtu
        assert length < 1500
        logging.debug(
            "first segment of bytes are(should be Sofh): data: {!r}".format(
                remaining_bytes[:6].hex(":")
            )
        )
        (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
        logging.debug(
            "len(remaining_bytes) after sofh: {}".format(len(remaining_bytes))
        )
        msg_bytes_consumed += Sofh.struct_size_bytes()
        logging.debug("length as read by sofh: {}".format(sofh.message_len()))
        assert sofh.message_len() == length
        assert sofh.message_body_len() <= len(remaining_bytes)
        # lets check the datetime
        # we need to consume the message header first
        (smh, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
        msg_bytes_consumed += SplineMessageHeader.struct_size_bytes()
        # if not at 4 byte alignment, do so now.
        pad_bytes = align_by(msg_bytes_consumed, 4)
        assert remaining_bytes[:pad_bytes] == b"\x00" * pad_bytes
        msg_bytes_consumed += pad_bytes
        logging.debug("{}".format(smh))
        # next should be a datetimestamp, the body of the curve message
        (spline_dttm, remaining_bytes) = SplineDateTime.deserialize(remaining_bytes)
        msg_bytes_consumed += SplineDateTime.struct_size_bytes()
        logging.debug("{}".format(spline_dttm))
        assert spline_dttm.to_datetime() == dt_tm + relativedelta(
            second=0, microsecond=0
        )

        # now should come the group
        (size_of_one,) = struct.unpack("<H", remaining_bytes[:2])
        assert size_of_one == 68
        (num_in_grp,) = struct.unpack("<H", remaining_bytes[2:4])
        assert num_in_grp == rem_grp_len_ans[0]
        rem_grp_len_ans = rem_grp_len_ans[1:]

        remaining_bytes = remaining_bytes[4:]
        msg_bytes_consumed += 4

        # now to try to get a curve from it
        (sc, remaining_bytes) = MuniCurve.deserialize(remaining_bytes)
        msg_bytes_consumed += MuniCurve.struct_size_bytes()
        logging.debug("spline curve:\n{}".format(sc))

        # skip to the next message
        curves = curves[1:]


def test_municurves_factory_deserialize_no_header(integration_test_data_dir) -> None:  # type: ignore[no-untyped-def]
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_municurves_factory_deserialize_no_header -o log_cli=true
    """
    initialize()
    itdd = Path(integration_test_data_dir)
    curves_20230511700_dir = itdd / "curves" / "2023" / "05" / "11" / "17" / "00"
    features = [
        "aaa_go_5_lrg_lqd",
        "aaa_go_5_med_lqd",
        "aaa_go_5_sml_lqd",
        "aaa_rev_5_lrg_lqd",
        "aaa_rev_5_med_lqd",
        "aaa_rev_5_sml_lqd",
        "a_go_5_lrg_lqd",
        "a_go_5_med_lqd",
        "a_go_5_sml_lqd",
        "a_rev_5_lrg_lqd",
        "a_rev_5_med_lqd",
        "a_rev_5_sml_lqd",
        "aa_go_5_lrg_lqd",
        "aa_go_5_med_lqd",
        "aa_go_5_sml_lqd",
        "aa_rev_5_lrg_lqd",
        "aa_rev_5_med_lqd",
        "aa_rev_5_sml_lqd",
        "bbb_go_5_lrg_lqd",
        "bbb_go_5_med_lqd",
        "bbb_go_5_sml_lqd",
        "bbb_rev_5_lrg_lqd",
        "bbb_rev_5_med_lqd",
        "bbb_rev_5_sml_lqd",
    ]
    dfs = []

    for feat in features:
        path = curves_20230511700_dir / feat / "curve.parquet"
        logging.debug("test path is: {}".format(path))
        dfs.append(pl.read_parquet(path))

    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2023, 5, 11, 17, 00)
    curves = [
        c
        for c in MuniCurvesMessageFactory.serialize_from_dataframes(
            dt_tm=dt_tm, feature_sets=features, dfs=dfs
        )
    ]
    # logging.debug("{!r}".format(bs.hex(":")))

    group_length_ans = [20, 4]
    # knowing what the layout should be, lets test different parts to verify

    logging.debug("len(curves)={}".format(len(curves)))
    curve_count = 0
    while len(curves) > 0:
        remaining_bytes = curves[0]
        logging.debug("curve_count start of loop={}".format(curve_count))
        logging.debug("len(remaining_bytes): {}".format(len(remaining_bytes)))
        msg_bytes_consumed = 0
        # first, get the sofh
        (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
        logging.debug(
            "len(remaining_bytes) after sofh: {}".format(len(remaining_bytes))
        )
        msg_bytes_consumed += Sofh.struct_size_bytes()
        # then immediately after follows the message header

        (smh, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
        msg_bytes_consumed += SplineMessageHeader.struct_size_bytes()
        # if not at 4 byte alignment, do so now.
        pad_bytes = align_by(msg_bytes_consumed, 4)
        assert remaining_bytes[:pad_bytes] == b"\x00" * pad_bytes
        msg_bytes_consumed += pad_bytes
        # then we know the msgid
        assert smh.template_id == 1
        assert smh.schema_id == 42
        assert smh.version == 0
        # great, the message type we knew was there. Deserialize now.
        (
            dttm_de,
            feature_sets_de,
            dfs_de,
            remaining_bytes,
        ) = MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(
            remaining_bytes
        )
        logging.debug("datetime: {}".format(dttm_de))
        logging.debug("feature_sets: {}".format(feature_sets_de))
        logging.debug("dataframes:\n{}".format(dfs_de))
        logging.debug("len(dataframes)={}".format(len(dfs_de)))
        logging.debug("remaining_byte length: {}".format(len(remaining_bytes)))
        assert dttm_de == dt_tm
        assert feature_sets_de == features[: group_length_ans[0]]
        features = features[group_length_ans[0] :]
        for df, df_de in zip(dfs[: group_length_ans[0]], dfs_de):
            logging.debug("source df:\n{}\ndeser_df:\n{}\n".format(df, df_de))
            assert df.equals(other=df_de)
        dfs = dfs[group_length_ans[0] :]
        group_length_ans = group_length_ans[1:]
        logging.debug("len(remaining_bytes)={}".format(len(remaining_bytes)))
        curves = curves[1:]

        curve_count += len(dfs_de)
        logging.debug("curve_count end of loop={}".format(curve_count))
