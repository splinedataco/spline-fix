import pytest
from joshua.fix import types as fix_types
from joshua.fix.messages.curves import MuniCurve
from joshua.fix.messages.featureset import (
    FeatureSet,
    MuniLiquidity,
    MuniPayment,
    MuniSize,
)
from joshua.fix.types import GroupSizeEncodingSimple, SplineByte, SplineByteData
import logging
import struct
import datetime as dt
from dateutil.relativedelta import relativedelta


def test_align_by() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_align_by -o log_cli=true
    """
    test_questions = [
        (4, 4),
        (3, 4),
        (2, 4),
        (1, 4),
        (0, 4),
        (8, 4),
        (7, 4),
        (6, 4),
        (5, 4),
        (2, 2),
        (1, 2),
        (4, 2),
        (3, 2),
    ]
    test_answers = [0, 1, 2, 3, 0, 0, 1, 2, 3, 0, 1, 0, 1]

    for q, a in zip(test_questions, test_answers):
        bytes_to_add = fix_types.align_by(numbytes=q[0], alignment_by=q[1])
        assert bytes_to_add == a


def test_group_size_encoding_simple_max_elements_allowed() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_max_elements_allowed -o log_cli=true
    """
    datetimestamp_size = 8
    values = [x for x in range(0, 30, 1)]
    simple_curve = MuniCurve(
        FeatureSet("aaa", MuniPayment.go, 5, MuniSize.lrg, MuniLiquidity.lqd),
        values,
    )
    curve_msg_size = len(simple_curve)
    # will therefore cause break here if the size of the message ever changes
    assert curve_msg_size == 68

    logging.debug("SimpleCurve size is: {}".format(len(simple_curve)))
    # NOTE: the reason the number of curves in aligned by 4 and 0 is the same
    # is because it is 68 bytes with is divisible by 4, and therefore already aligned
    questions = [
        (simple_curve, datetimestamp_size, 4),
        (simple_curve, 0, 4),
        (simple_curve, datetimestamp_size, 0),
        (simple_curve, datetimestamp_size, 8),
        (simple_curve, 0, 8),
    ]
    answers = [21, 21, 21, 19, 19]

    for q, a in zip(questions, answers):
        ans = GroupSizeEncodingSimple.max_elements_allowed(
            single_element=q[0], additional_byte_restrict=q[1], alignment_by=q[2]  # type: ignore[arg-type]
        )
        assert ans == a


def test_group_size_encoding_simple_construct_one() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_one -o log_cli=true
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

    curve_list = [mc0]
    curve_group = GroupSizeEncodingSimple(
        data=curve_list, additional_byte_restrict=8, align_to=4
    )
    logging.debug("single curve in group: {}".format(curve_group))
    assert curve_group.MAX_LEN == 1436
    assert curve_group.NUM_BYTES == 4
    assert curve_group.num_in_group.value == 1
    assert curve_group.block_length == 68
    assert curve_group.struct_size_bytes() == 4
    assert curve_group.static_block_length(MuniCurve, align_to=4) == 68
    assert len(curve_group) == mc0_size + 4
    assert curve_group.leader_padding == 0


def test_group_size_encoding_simple_construct_one_align_at_eight() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_one_align_at_eight -o log_cli=true
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

    curve_list = [mc0]
    curve_group = GroupSizeEncodingSimple(
        data=curve_list, additional_byte_restrict=8, align_to=8
    )
    logging.debug("single curve in group: {}".format(curve_group))
    assert curve_group.leader_padding == 4
    assert curve_group.MAX_LEN == 1436
    assert curve_group.NUM_BYTES == 4
    assert curve_group.num_in_group.value == 1
    # size is 72 now because 68 isn't evenly divisible by 8, so it gets an extra 4
    # for a group, the sizeof(block length) is 2
    # then there is the sizeof(num_in_group) which is 2 bytes,
    # and then one row 72 * num_in_group = 1
    # LEADER(4) + LEADER_PADDING(4) + (PADDED_ELEMENT(72) * num_in_group)
    # 4 + 4 + (72 * 1) = 80
    assert curve_group.length_in_bytes() == 80
    # for a group, the block length is actually the size of 1 row
    assert curve_group.static_block_length(subtype=MuniCurve, align_to=8) == 72
    assert curve_group.block_length == 72
    assert curve_group.struct_size_bytes() == 4
    # this is testing the size of the whole thing
    # so size of the curve=68, + 4 bytes for group leader,
    assert len(curve_group) == mc0_size + 4 + 4 + 4


def test_group_size_encoding_simple_construct_fits_in_one_packet() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_fits_in_one_packet -o log_cli=true
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

    curve_list = [mc0 for _ in range(0, 20, 1)]
    curve_group = GroupSizeEncodingSimple(
        data=curve_list, additional_byte_restrict=8, align_to=4
    )
    logging.debug("single curve in group: {}".format(curve_group))
    assert curve_group.MAX_LEN == 1436
    assert curve_group.NUM_BYTES == 4
    assert curve_group.num_in_group.value == 20
    # size of 1 curve*20 + info needed to describe a group
    assert curve_group._block_length.value == mc0_size + curve_group.alignment_padding
    # size of 1 curve*20 + info needed to describe a group
    assert len(curve_group) == (20 * mc0_size) + 4


def test_group_size_encoding_simple_construct_fits_in_one_packet_aligned_at_eight() -> (
    None
):
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_fits_in_one_packet_aligned_at_eight  -o log_cli=true
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

    curve_list = [mc0 for _ in range(0, 19, 1)]
    curve_group = GroupSizeEncodingSimple(
        data=curve_list, additional_byte_restrict=8, align_to=8
    )
    logging.debug("single curve in group: {}".format(curve_group))
    assert curve_group.MAX_LEN == 1436
    assert curve_group.NUM_BYTES == 4
    assert curve_group.num_in_group.value == 19
    # size of 1 curve*19 + info needed to describe a group
    assert curve_group._block_length.value == 72
    assert curve_group.block_length == 72
    assert curve_group.struct_size_bytes() == 4
    # size of 1 element * number of elements + group leader + padding to 8 bytes
    assert curve_group.length_in_bytes() == (72 * 19) + 4 + 4
    # size of 1 curve*19 + info needed to describe a group
    # (actual size + 4 byte padding) * num elements + group leader + leader padding
    assert len(curve_group) == (19 * (mc0_size + 4)) + 4 + 4


def test_group_size_encoding_simple_construct_doesnt_fit_in_one_packet() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_doesnt_fit_in_one_packet -o log_cli=true
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

    total_curves = 22
    curve_list = [mc0 for _ in range(0, total_curves, 1)]
    # should fail if we try all in one
    with pytest.raises(ValueError) as exinfo:
        _curve_group = GroupSizeEncodingSimple(
            data=curve_list, additional_byte_restrict=8, align_to=4
        )
    assert str(exinfo.value) == "elements are too large to fit in single MTU"

    # now again, but we'll split it into multiple parts
    max_len = GroupSizeEncodingSimple.max_elements_allowed(
        curve_list[0], additional_byte_restrict=8, alignment_by=4  # type: ignore[arg-type]
    )
    assert max_len == 21
    curve_group0 = GroupSizeEncodingSimple(
        data=curve_list[:max_len], additional_byte_restrict=8, align_to=4
    )
    logging.debug("multiple curves in group 1/2: {}".format(curve_group0))
    assert curve_group0.num_in_group.value == max_len
    # size of 1 curve*20 + info needed to describe a group
    assert curve_group0._block_length.value == 68
    assert curve_group0.length_in_bytes() == (max_len * 68) + 4
    # size of 1 curve*20 + info needed to describe a group
    assert len(curve_group0) == (max_len * mc0_size) + 4
    curve_group1 = GroupSizeEncodingSimple(
        data=curve_list[max_len:], additional_byte_restrict=8, align_to=4
    )
    logging.debug("multiple curves in group 2/2: {}".format(curve_group1))
    assert curve_group1.num_in_group.value == total_curves - max_len
    # size of 1 curve + info needed to describe a group
    assert curve_group1._block_length.value == (1 * 68)
    assert curve_group1.length_in_bytes() == (1 * 68) + 4
    # size of 1 curve + info needed to describe a group
    assert len(curve_group1) == (1 * mc0_size) + 4


def test_group_size_encoding_simple_construct_doesnt_fit_in_one_packet_aligned_to_eight() -> (
    None
):
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_size_encoding_simple_construct_doesnt_fit_in_one_packet_aligned_to_eight -o log_cli=true
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

    num_curves = 20
    curve_list = [mc0 for _ in range(0, num_curves, 1)]
    # should fail if we try all in one
    with pytest.raises(ValueError) as exinfo:
        _curve_group = GroupSizeEncodingSimple(
            data=curve_list, additional_byte_restrict=8, align_to=8
        )
    assert str(exinfo.value) == "elements are too large to fit in single MTU"

    # now again, but we'll split it into multiple parts
    max_len = GroupSizeEncodingSimple.max_elements_allowed(
        curve_list[0], additional_byte_restrict=8, alignment_by=8  # type: ignore[arg-type]
    )
    assert max_len == num_curves - 1
    curve_group0 = GroupSizeEncodingSimple(
        data=curve_list[:max_len], additional_byte_restrict=8, align_to=8
    )
    logging.debug("multiple curves in group 1/2: {}".format(curve_group0))
    assert curve_group0.num_in_group.value == num_curves - 1
    # size of 1 curve*20 + info needed to describe a group
    assert curve_group0.length_in_bytes() == ((num_curves - 1) * 72) + 4 + 4
    assert curve_group0._block_length.value == 72
    assert curve_group0.block_length == 72
    assert curve_group0.struct_size_bytes() == 4
    # size of 1 curve*20 + info needed to describe a group
    assert len(curve_group0) == ((num_curves - 1) * (mc0_size + 4)) + 4 + 4
    curve_group1 = GroupSizeEncodingSimple(
        data=curve_list[max_len:], additional_byte_restrict=8, align_to=8
    )
    logging.debug("multiple curves in group 2/2: {}".format(curve_group1))
    assert curve_group1.num_in_group.value == 1
    # size of 1 curve + info needed to describe a group
    assert curve_group1.length_in_bytes() == (1 * 72) + 4 + 4
    assert curve_group1._block_length.value == 72

    # size of 1 curve + info needed to describe a group
    assert len(curve_group1) == (1 * (mc0_size + 4)) + 4 + 4


def test_group_municurve_serialize_one() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_municurve_serialize_one -o log_cli=true
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
    curve_group = GroupSizeEncodingSimple(
        data=[mc0], additional_byte_restrict=8, align_to=4
    )
    serialized_group = curve_group.serialize()
    logging.debug("serialized_group: '{!r}'".format(serialized_group))
    answer = b"D\x00\x01\x00AAA\x00\x01\x05\x03\x03G\n\xd6\tc\t\xee\x08\xd9\x08\xf2\x0b\x17\x0c<\x0cb\x0cy\xf3G\n\xd6\tc\t\xee\x08\xd9\x08\xf2\x0b\x17\x0c<\x0cb\x0cy\xf3G\n\xd6\tc\t\xee\x08\xd9\x08\xf2\x0b\x17\x0c<\x0cb\x0cy\xf3"
    assert serialized_group == answer
    # group structure is 2 bytes for length of 1 element, and 2 bytes for number of elements
    (len_one_elem,) = struct.unpack("<H", serialized_group[0:2])
    # checking against just the element list, since there is just 1 element in this test
    assert len_one_elem == len(answer[4:])
    (num_elements,) = struct.unpack("<H", serialized_group[2:4])
    assert num_elements == 1
    # no padding with align_to=4
    # so, first message.
    # 4 bytes for rating
    (b_rating,) = struct.unpack("<4s", serialized_group[4:8])
    last = 8
    first_null = b_rating.find(b"\x00")
    rating = b_rating[0:first_null].decode("latin1")
    assert rating == "AAA"
    # then payment, coupon, muni_size, and liquidity
    payment = MuniPayment(struct.unpack("<B", serialized_group[last : last + 1])[0])
    last += 1
    coupon = struct.unpack("<B", serialized_group[last : last + 1])[0]
    last += 1
    muni_size = MuniSize(struct.unpack("<B", serialized_group[last : last + 1])[0])
    last += 1
    liquidity = MuniLiquidity(struct.unpack("<B", serialized_group[last : last + 1])[0])
    assert payment == MuniPayment.go
    assert coupon == 5
    assert muni_size == MuniSize.lrg
    assert liquidity == MuniLiquidity.lqd


def test_group_municurve_deserialize_one() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_municurve_deserialize_one -o log_cli=true
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
    curve_group = GroupSizeEncodingSimple(
        data=[mc0], additional_byte_restrict=8, align_to=4
    )
    serialized_group = curve_group.serialize()
    assert len(serialized_group) == 72
    (stuff, remaining_bytes) = GroupSizeEncodingSimple.deserialize(
        data=serialized_group, group_type=MuniCurve, align_to=4
    )
    logging.debug("deserialized group:")
    for c in stuff:
        logging.debug("curve: {}".format(c))

        tst_curve = c
        tst_features = tst_curve.features
        assert tst_features.rating == "AAA"
        assert tst_features.payment == MuniPayment.go
        assert tst_features.coupon == 5
        assert tst_features.muni_size == MuniSize.lrg
        assert tst_features.liquidity == MuniLiquidity.lqd


def test_group_municurve_deserialize_many() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_group_municurve_deserialize_many -o log_cli=true
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
    curves_in = [mc0 for _ in range(0, 19, 1)]
    curve_group = GroupSizeEncodingSimple(
        data=curves_in, additional_byte_restrict=8, align_to=4
    )
    serialized_group = curve_group.serialize()
    assert len(serialized_group) == 1296
    # group structure is 2 bytes length of one element, 2 bytes number of elements
    (len_of_one_elem,) = struct.unpack("<H", serialized_group[0:2])
    assert len_of_one_elem == 68
    (num_elements,) = struct.unpack("<H", serialized_group[2:4])
    assert num_elements == 19
    # check the length is correct
    assert 1296 == len_of_one_elem * num_elements + 4
    (stuff, remaining_bytes) = GroupSizeEncodingSimple.deserialize(
        data=serialized_group, group_type=MuniCurve, align_to=4
    )
    logging.debug("deserialized group: {}".format(stuff))
    for c in stuff:
        tst_curve = c

        tst_features = tst_curve.features
        assert tst_features.rating == "AAA"
        assert tst_features.payment == MuniPayment.go
        assert tst_features.coupon == 5
        assert tst_features.muni_size == MuniSize.lrg
        assert tst_features.liquidity == MuniLiquidity.lqd


def test_spline_datetime_serialize_deserialize() -> None:
    test_datetimes = [
        dt.datetime(2001, 9, 15, 11, 15),
        dt.datetime(2001, 8, 17, 0, 0),
        dt.datetime(2001, 10, 15, 2, 15),
        dt.datetime.now(),
    ]

    for td in test_datetimes:
        logging.debug(f"start with: {td}")
        sdt = fix_types.SplineDateTime(td)
        logging.debug("SplineDateTime.str: {}".format(str(sdt)))
        logging.debug("SplineDateTime.repr: {}".format(repr(sdt)))
        logging.debug(f"internal datetime as int(float): {sdt._timestamp}")
        sdt_ser = sdt.serialize()
        logging.debug(
            "SplineDatetime.serialized: {!r}".format(sdt_ser.hex(":", bytes_per_sep=8))
        )
        (sdt_deser, remaining_bytes) = fix_types.SplineDateTime.deserialize(sdt_ser)
        logging.debug(f"internal after deser: {sdt_deser._timestamp}")
        assert remaining_bytes == bytes()
        assert sdt_deser == sdt
        trunc_td = td + relativedelta(microsecond=0)
        assert sdt_deser.to_datetime() == trunc_td


def test_spline_nanotime_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_spline_nanotime_serialize_deserialize -o log_cli=true
    """
    test_datetimes = [
        dt.datetime(2001, 9, 15, 11, 15, 31, 123),
        dt.datetime(2001, 8, 17, 0, 0),
        dt.datetime(2001, 10, 15, 2, 15, 42, 876),
        dt.datetime.now(),
    ]

    for td in test_datetimes:
        sdt = fix_types.SplineNanoTime(td)
        logging.debug("SplineNanoTime.str: {}".format(str(sdt)))
        logging.debug("SplineNanoTime.repr: {}".format(repr(sdt)))
        sdt_ser = sdt.serialize()
        logging.debug(
            "SplineNanoTime.serialized: {!r}".format(sdt_ser.hex(":", bytes_per_sep=8))
        )
        (sdt_deser, remaining_bytes) = fix_types.SplineNanoTime.deserialize(sdt_ser)
        assert remaining_bytes == bytes()
        assert sdt_deser == sdt
        assert sdt_deser.to_datetime() == td


def test_spline_byte_data() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_spline_byte_data -o log_cli=true
    """
    test_str = "the quick brown fox jumped over the lazy dog!"
    logging.debug("test str len: {}".format(len(test_str)))
    test_bytes = test_str.encode("utf8")
    logging.debug("test bytes len: {}".format(len(test_bytes)))

    sbd = SplineByteData(data=test_bytes, align_to=1)
    logging.debug("SplineByteData: {}".format(sbd))

    sbd_ser = sbd.serialize()
    logging.debug("type(sbd_ser): {}".format(type(sbd_ser)))
    logging.debug("sbd_ser: {!r}".format(sbd_ser.hex(":")))
    (sbd_deser, remaining_bytes) = SplineByteData.deserialize(
        group_type=SplineByte, data=sbd_ser
    )
    logging.debug("type(sbd_deser): {}".format(type(sbd_deser)))
    logging.debug("sbd_deser: {}".format(sbd_deser))
