import pytest
from joshua.fix import sofh
from joshua.fix.sbe import sbe_little_endian


def test_sofh_default() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sofh_default -o log_cli=true
    """
    sofh_default = sofh.Sofh()
    assert repr(sofh_default) == "00:00:00:06:eb:50"
    assert str(sofh_default) == "size: 6 encoding: b'\\xebP'"
    assert sofh_default.size == 6
    assert sofh_default.encoding == sbe_little_endian


def test_sofh_size() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sofh_size -o log_cli=true
    """
    sofh_size = sofh.Sofh(size=4200)
    assert repr(sofh_size) == "00:00:10:6e:eb:50"
    assert str(sofh_size) == "size: 4206 encoding: b'\\xebP'"

    with pytest.raises(ValueError) as excinfo:
        sofh.Sofh(size=2**32 - 5)

    assert str(excinfo.value) == "4294967291 + 6 is too big to fit in 2^32"


def test_sofh_encoding() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sofh_encoding -o log_cli=true
    """
    sofh_enc = sofh.Sofh()
    assert sofh_enc.encoding == sbe_little_endian

    with pytest.raises(ValueError) as excinfo:
        sofh.Sofh(enc=b"12345")

    assert str(excinfo.value) == "31:32:33:34:35 needs to be exactly 2 bytes"

    with pytest.raises(ValueError) as excinfo:
        sofh.Sofh(enc=b"1")

    assert str(excinfo.value) == "31 needs to be exactly 2 bytes"

    not_real_acceptable_enc = sofh.Sofh(enc=b"42")
    assert not_real_acceptable_enc.encoding == b"42"


def test_sofh_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sofh_serialize -o log_cli=true
    """
    sofh_default = sofh.Sofh()
    sofh_ser = sofh_default.serialize()
    assert sofh_ser.hex(":") == repr(sofh_default)
    sofh_ser = sofh.Sofh(size=42).serialize()
    assert sofh_ser.hex(":") == "00:00:00:30:eb:50"

    print("{}".format(sofh_ser.hex(":")))


def test_sofh_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_sofh_deserialize -o log_cli=true
    """
    serialized_sofh = b"\x00\x00\x00\x06\xebP"
    (sofh_deserialized, remainder) = sofh.Sofh.deserialize(serialized_sofh)
    print("deser default:\n{}".format(sofh_deserialized))
    assert sofh_deserialized == sofh.Sofh(size=0)
    assert remainder == bytes()
    serialized_sofh = b"\x00\x00\x00\x30\xebP"
    (sofh_deserialized, remainder) = sofh.Sofh.deserialize(serialized_sofh)
    assert sofh_deserialized == sofh.Sofh(size=42)
    assert repr(sofh_deserialized) == "00:00:00:30:eb:50"
    assert remainder == bytes()
    assert sofh_deserialized.message_body_len() == 42
    assert sofh_deserialized.message_len() == 42 + sofh_deserialized.NUM_BYTES
    assert len(sofh_deserialized) == sofh_deserialized.NUM_BYTES


def test_sofh_find_sofh() -> None:
    serialized_sofh = b"\x00\x00\x00\x06\xebP000000000000000000"
    pos = sofh.Sofh.find_sofh(data=serialized_sofh)
    assert pos == 0

    serialized_sofh = b"asg978as97anglhoashg\x00\x00\x00\x06\xebP000000000000000000"
    pos = sofh.Sofh.find_sofh(data=serialized_sofh)
    assert pos == 20
