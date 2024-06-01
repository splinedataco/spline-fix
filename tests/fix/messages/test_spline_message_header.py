import pytest
from joshua.fix.messages import SplineMessageHeader
from joshua.fix.messages.curves import MuniCurve
import logging
import struct


def test_spline_message_header_construct() -> None:
    block_lengths = [MuniCurve.struct_size_bytes(), 68, 72, 100]
    template_ids = [1, 2, 3, 4]
    schema_ids = [42, 0, 10, 100]
    versions = [0, 2, 3, 9]
    num_groupss = [1, 10, 100, 255]

    for block_length, template_id, schema_id, version, num_groups in zip(
        block_lengths, template_ids, schema_ids, versions, num_groupss
    ):
        smh = SplineMessageHeader(
            block_length=block_length,
            template_id=template_id,
            schema_id=schema_id,
            version=version,
            num_groups=num_groups,
        )

        assert smh.block_length == block_length
        assert smh.template_id == template_id
        assert smh.schema_id == schema_id
        assert smh.version == version
        assert smh.num_groups == num_groups
        assert smh.struct_size_bytes() == smh.NUM_BYTES

        logging.debug("smh:\n{}".format(str(smh)))

    # have only 1 thing broken per row
    block_lengths = [65537, MuniCurve.struct_size_bytes(), 68, 72, 100, 0]
    template_ids = [0, 257, 2, 3, 4, 0]
    schema_ids = [42, 42, 1000, 10, 100, 0]
    versions = [10, 0, 2, 500, 9, 0]
    num_groupss = [50, 1, 10, 100, 1337, -1]
    exceptions = [
        f"block_length is too large to fit in 2**16. Got: {65537}",
        "template_id must be a non-negative number less than 256. Got: 257",
        "schema_id must be a non-negative number less than 256. Got: 1000",
        "version must be a non-negative number less than 256. Got: 500",
        "num_groups must be a non-negative number less than 256. Got: 1337",
        "num_groups must be a non-negative number less than 256. Got: -1",
    ]

    for block_length, template_id, schema_id, version, num_groups, expect_except in zip(
        block_lengths, template_ids, schema_ids, versions, num_groupss, exceptions
    ):
        with pytest.raises(ValueError) as excinfo:
            SplineMessageHeader(
                block_length=block_length,
                template_id=template_id,
                schema_id=schema_id,
                version=version,
                num_groups=num_groups,
            )
        logging.debug("exception:\n{}".format(excinfo))
        assert str(excinfo.value) == expect_except


def test_spline_message_header_serialize() -> None:
    """Simple one to make sure that it looks as we expect
    and aren't just able to deserialize because we made the same
    error symmetrically.
    """
    block_length = MuniCurve.struct_size_bytes()
    template_id = 1
    schema_id = 42
    version = 0
    num_groups = 1

    smh_0 = SplineMessageHeader(
        block_length=block_length,
        template_id=template_id,
        schema_id=schema_id,
        version=version,
        num_groups=num_groups,
    )
    ans_serialized = b"D\x00\x01*\x00\x01"
    smh_bytes = smh_0.serialize()
    assert smh_bytes == ans_serialized
    # Format should be
    # block length, uint16
    (deser_block_length,) = struct.unpack("<H", smh_bytes[0:2])
    assert deser_block_length == block_length
    # then template_id as u8
    (deser_template_id,) = struct.unpack("<B", smh_bytes[2:3])
    assert deser_template_id == template_id
    # then schema_id as u8
    (deser_schema_id,) = struct.unpack("<B", smh_bytes[3:4])
    assert deser_schema_id == schema_id
    # then version as u8
    (deser_version,) = struct.unpack("<B", smh_bytes[4:5])
    assert deser_version == version
    # then num_groups as u8
    (deser_num_groups,) = struct.unpack("<B", smh_bytes[5:6])
    assert deser_num_groups == num_groups

    (smh, remaining_bytes) = SplineMessageHeader.deserialize(smh_bytes)

    print(f"{smh}")
    assert smh_0 == smh
    assert remaining_bytes == bytes()
    assert smh.block_length == block_length
    assert smh.template_id == template_id
    assert smh.schema_id == schema_id
    assert smh.version == version
    assert smh.num_groups == num_groups
    assert smh.struct_size_bytes() == smh.NUM_BYTES

    logging.debug("smh:\n{}".format(str(smh)))


def test_spline_message_header_serialize_deserialize() -> None:
    block_lengths = [MuniCurve.struct_size_bytes(), 68, 72, 100]
    template_ids = [1, 2, 3, 4]
    schema_ids = [42, 0, 10, 100]
    versions = [0, 2, 3, 9]
    num_groupss = [1, 10, 100, 255]

    for block_length, template_id, schema_id, version, num_groups in zip(
        block_lengths, template_ids, schema_ids, versions, num_groupss
    ):
        smh_0 = SplineMessageHeader(
            block_length=block_length,
            template_id=template_id,
            schema_id=schema_id,
            version=version,
            num_groups=num_groups,
        )
        smh_bytes = smh_0.serialize()
        (smh, remaining_bytes) = SplineMessageHeader.deserialize(smh_bytes)

        print(f"{smh}")
        assert smh_0 == smh
        assert remaining_bytes == bytes()
        assert smh.block_length == block_length
        assert smh.template_id == template_id
        assert smh.schema_id == schema_id
        assert smh.version == version
        assert smh.num_groups == num_groups
        assert smh.struct_size_bytes() == smh.NUM_BYTES

        logging.debug("smh:\n{}".format(str(smh)))
