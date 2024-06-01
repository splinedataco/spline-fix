import pytest
from joshua.fix import sbe


def test_sbe_init() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sbe_init -o log_cli=true
    """
    one_sbe = sbe.SbeMessageEncodingHeader(42, 0, 1, 2, 3, 4)

    assert one_sbe.block_length == 42
    assert one_sbe.template_id == 0
    assert one_sbe.schema_id == 1
    assert one_sbe.version == 2
    assert one_sbe.num_groups == 3
    assert one_sbe.num_var_data_fields == 4

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(2**16 + 1, 0, 0, 0, 0, 0)
    assert str(exinfo.value) == "block_length needs to fit within 2^2 but got 65537"

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(0, 2**16 + 1, 0, 0, 0, 0)
    assert str(exinfo.value) == "template_id needs to fit within 2^2 but got 65537"

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(0, 0, 2**16 + 1, 0, 0, 0)
    assert str(exinfo.value) == "schema_id needs to fit within 2^2 but got 65537"

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(0, 0, 0, 2**16 + 1, 0, 0)
    assert str(exinfo.value) == "version needs to fit within 2^2 but got 65537"

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(0, 0, 0, 0, 2**16 + 1, 0)
    assert str(exinfo.value) == "num_groups needs to fit within 2^2 but got 65537"

    with pytest.raises(ValueError) as exinfo:
        one_sbe = sbe.SbeMessageEncodingHeader(0, 0, 0, 0, 0, 2**16 + 1)
    assert (
        str(exinfo.value) == "num_var_data_fields needs to fit within 2^2 but got 65537"
    )


def test_sbe_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sbe_serialize -o log_cli=true
    """
    one_sbe = sbe.SbeMessageEncodingHeader(1, 2, 3, 4, 5, 6)
    # we will always do little endian because we're defining the protocol and it's
    # the native byte order
    assert one_sbe.serialize() == b"\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00"

    two_sbe = sbe.SbeMessageEncodingHeader(42, 57, 0, 100, 1111, 2222)
    assert two_sbe.serialize().hex(":") == "2a:00:39:00:00:00:64:00:57:04:ae:08"


def test_sbe_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sbe_deserialize -o log_cli=true
    """
    one_sbe = sbe.SbeMessageEncodingHeader(1, 2, 3, 4, 5, 6)
    # we will always do little endian because we're defining the protocol and it's
    # the native byte order
    one_serialized_sbe = b"\x01\x00\x02\x00\x03\x00\x04\x00\x05\x00\x06\x00"
    (one_deser_sbe, rem) = sbe.SbeMessageEncodingHeader.deserialize(one_serialized_sbe)
    assert one_deser_sbe == one_sbe

    # two_sbe = sbe.SbeMessageEncodingHeader(42, 57, 0, 100, 1111, 2222)
    # assert two_sbe.serialize().hex(":") == "2a:00:39:00:00:00:64:00:57:04:ae:08"
