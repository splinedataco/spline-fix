from typing import cast
from joshua.fix.common import align_by, FixpMessageType
from joshua.fix.types import (
    SplineByteData,
    SplineNanoTime,
)
from joshua.fix.fixp.messages import (
    Uuid,
)
from joshua.fix.fixp.establish import (
    Establish,
    EstablishmentAck,
    EstablishmentReject,
    EstablishmentRejectCodeEnum,
    REJECTION_REASON_STRINGS,
)
import logging
import datetime as dt
import struct
import time


def get_timestamp() -> float:
    timestamp: float
    while True:
        timestamp = dt.datetime.now().timestamp()
        timestamp_i = int(timestamp * 1_000_000_000)
        timestamp_f = timestamp_i / 1_000_000_000.0
        if timestamp == timestamp_f:
            break
        logging.debug(
            "timestamp={} != {} = reconstituted timestamp".format(
                timestamp, timestamp_f
            )
        )
        time.sleep(1)
    return timestamp


def test_fixp_establish_create() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_establish_create -o log_cli=true
    """
    while True:
        timestamp = dt.datetime.now().timestamp()
        timestamp_i = int(timestamp * 1_000_000_000)
        timestamp_f = timestamp_i / 1_000_000_000.0
        if timestamp == timestamp_f:
            break
        logging.debug(
            "timestamp={} != {} = reconstituted timestamp".format(
                timestamp, timestamp_f
            )
        )
        time.sleep(1)

    spline_uuid = Uuid()

    # now we have enough to make our Establish message
    my_name_is_my_passport = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=timestamp,
        keep_alive_ms=1_000,
    )
    logging.debug("Establish msg:\n{}".format(my_name_is_my_passport))
    assert my_name_is_my_passport._sessionId == spline_uuid
    assert my_name_is_my_passport._messageType == FixpMessageType.Establish
    assert my_name_is_my_passport.credentials is None
    assert my_name_is_my_passport.timestamp_nano == SplineNanoTime(timestamp)
    assert my_name_is_my_passport.keep_alive == 1_000


def test_fixp_establish_serialize_deserialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_establish_serialize -o log_cli=true
    """
    # from the client point of view
    # we need to negotiate a connection and identify ourselves
    # we'll identify by getting a jwt from our login server first

    # we'll make a timestamp so we can test, if not given will be created
    # for us

    timestamp = get_timestamp()
    spline_uuid = Uuid()
    keep_alive_ms = 1_000

    # now we have enough to make our Establish message
    my_name_is_my_passport = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=timestamp,
        keep_alive_ms=keep_alive_ms,
    )

    logging.debug("my_name_is_my_passport: {}".format(my_name_is_my_passport))
    establish_bytes = my_name_is_my_passport.serialize()
    logging.debug("len(establish_bytes)={}".format(len(establish_bytes)))
    logging.debug(
        "Establish.struct_size_bytes()={}".format(Establish.struct_size_bytes())
    )

    # manually decode
    remaining_bytes = establish_bytes
    logging.debug(
        "len(remaining_bytes)={} bytes: {!r}".format(
            len(remaining_bytes), remaining_bytes.hex(":")
        )
    )
    # should be sessionId/uuid
    (uuid_deser, extra) = Uuid.deserialize(remaining_bytes[0:16])
    pos = 16
    assert extra == bytes()
    assert uuid_deser == spline_uuid
    logging.debug(
        "len(remaining_bytes)={} bytes: {!r}".format(
            len(remaining_bytes), remaining_bytes[pos:].hex(":")
        )
    )

    # then nanotime
    (nano_deser, extra) = SplineNanoTime.deserialize(remaining_bytes[pos:24])
    pos = 24
    nano_deser = cast(SplineNanoTime, nano_deser)
    assert extra == bytes()
    assert nano_deser.timestamp == timestamp
    logging.debug(
        "len(remaining_bytes)={} bytes: {!r}".format(
            len(remaining_bytes), remaining_bytes[pos:].hex(":")
        )
    )
    # u32 for keep alive
    (keep_alive_ms_int,) = struct.unpack("<L", remaining_bytes[pos:28])
    pos = 28
    logging.debug(
        "len(remaining_bytes)={} bytes: {!r}".format(
            len(remaining_bytes), remaining_bytes[pos:].hex(":")
        )
    )
    # u64 for next seq num
    (next_seq_num_int,) = struct.unpack("<Q", remaining_bytes[pos : pos + 8])
    pos += 8
    logging.debug(
        "len(remaining_bytes)={} bytes: {!r}".format(
            len(remaining_bytes), remaining_bytes[pos:].hex(":")
        )
    )
    # padding of ?
    padding = align_by(numbytes=Establish.struct_size_bytes())
    for i in range(0, padding):
        b = remaining_bytes[pos + i : pos + 1 + i]
        assert b == b"\x00"
    remaining_bytes = remaining_bytes[pos + padding :]
    (dummy, remaining_bytes) = SplineByteData.deserialize(
        remaining_bytes, group_type=bytes
    )
    assert remaining_bytes == bytes()
    assert dummy == bytes()

    # now try all together
    remaining_bytes = establish_bytes
    (establish_deser, remaining_bytes) = Establish.deserialize(establish_bytes)
    assert remaining_bytes == bytes()
    assert establish_deser == my_name_is_my_passport, "please verify me. Fail."


def test_fixp_establish_reject_create_serde() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_establish_create_serde -o log_cli=true
    """
    # from the client point of view
    # client has not negotiated a connection

    # we'll make a timestamp so we can test, if not given will be created
    # for us

    timestamp = get_timestamp()
    spline_timestamp = SplineNanoTime(timestamp)
    spline_uuid = Uuid()
    keep_alive_ms = 1_000

    # now we have enough to make our Establish message
    my_name_is_my_passport = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=timestamp,
        keep_alive_ms=keep_alive_ms,
    )

    logging.debug("my_name_is_my_passport: {}".format(my_name_is_my_passport))
    # establish_bytes = my_name_is_my_passport.serialize()

    reasons = [EstablishmentRejectCodeEnum(x) for x in range(0, 6)]
    for reject_reason in reasons:
        # from the Establish we can create a reject
        establish_reject = EstablishmentReject.from_establish(
            est=my_name_is_my_passport,
            reject_code=EstablishmentRejectCodeEnum(reject_reason),
        )
        establish_reject = cast(EstablishmentReject, establish_reject)
        establish_reject_bytes = establish_reject.serialize()
        # manually decode
        remaining_bytes = establish_reject_bytes

        # should be sessionId/uuid
        (uuid_deser, extra) = Uuid.deserialize(remaining_bytes[0:16])
        assert extra == bytes()
        assert uuid_deser == spline_uuid
        # then nanotime
        (nano_deser, extra) = SplineNanoTime.deserialize(remaining_bytes[16:24])
        assert extra == bytes()
        assert nano_deser == spline_timestamp
        # just 1 byte for serverFlow
        (erc_int_deser,) = struct.unpack("<B", remaining_bytes[24:25])
        erc_deser = EstablishmentRejectCodeEnum(erc_int_deser)
        assert extra == bytes()
        assert erc_deser == reject_reason
        # padding of ?, should be 0
        padding = EstablishmentReject._group_pad
        tst_pad = align_by(numbytes=EstablishmentReject.struct_size_bytes())
        assert padding == tst_pad + padding
        for i in range(0, padding):
            b = remaining_bytes[25 + i : 25 + 1 + i]
            assert b == b"\x00"
        pos = 25 + padding
        # for now, this should be empty
        (data_deser, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes[pos:], bytes
        )
        assert remaining_bytes == bytes()
        data_bytes = (REJECTION_REASON_STRINGS[erc_int_deser] + "").encode("latin-1")
        assert data_deser == data_bytes

        # now try all together
        (est_rej_deser, remaining_bytes) = EstablishmentReject.deserialize(
            establish_reject_bytes
        )
        assert remaining_bytes == bytes()
        assert est_rej_deser == establish_reject, "please identify me. Denied."

        assert est_rej_deser._sessionId == spline_uuid
        assert est_rej_deser._messageType == FixpMessageType.EstablishmentReject
        assert est_rej_deser.request_timestamp_nano == SplineNanoTime(timestamp)
        assert est_rej_deser._reject_code == reject_reason
        unspecified_reject_str = (
            REJECTION_REASON_STRINGS[est_rej_deser._reject_code.value] + ""
        )
        assert est_rej_deser._reject_str == SplineByteData(
            data=unspecified_reject_str.encode("latin-1")
        )


def test_fixp_establishment_ack_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_establishment_ack_serialize -o log_cli=true
    """
    # from the client point of view
    # client has already successfully negotiated and now is establishing
    # the session, and has sent an establish message.

    # we'll make a timestamp so we can test, if not given will be created
    timestamp = get_timestamp()
    spline_timestamp = SplineNanoTime(timestamp)
    spline_uuid = Uuid()
    keep_alive_ms = 1_000

    # now we have enough to make our Establish message
    my_name_is_my_passport = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=timestamp,
        keep_alive_ms=keep_alive_ms,
    )

    logging.debug("my_name_is_my_passport: {}".format(my_name_is_my_passport))
    # respond to client that sent the preceding establish message
    est_ack = EstablishmentAck.from_establish(my_name_is_my_passport, server_seq_num=0)
    est_ack = cast(EstablishmentAck, est_ack)
    est_ack_bytes = est_ack.serialize()

    remaining_bytes = est_ack_bytes
    # 16 bytes
    (sessionId, remaining_bytes) = Uuid.deserialize(ba=remaining_bytes)
    # 8 bytes
    (request_timestamp, remaining_bytes) = SplineNanoTime.deserialize(remaining_bytes)
    assert request_timestamp == spline_timestamp
    (keep_alive_int,) = struct.unpack("<L", remaining_bytes[0:4])
    assert keep_alive_int == keep_alive_ms
    remaining_bytes = remaining_bytes[4:]
    (next_seq_int,) = struct.unpack("<Q", remaining_bytes[0:8])
    assert next_seq_int == 1
    remaining_bytes = remaining_bytes[8:]
    padding = align_by(EstablishmentAck.struct_size_bytes())
    for b in remaining_bytes[:padding]:
        assert b == b"\x00"

    remaining_bytes = remaining_bytes[padding:]
    assert remaining_bytes == bytes()


def test_fixp_establishment_ack_serde() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_establishment_ack_serde -o log_cli=true
    """
    # from the client point of view
    # client has already successfully negotiated and now is establishing
    # the session, and has sent an establish message.

    # we'll make a timestamp so we can test, if not given will be created
    timestamp = get_timestamp()
    spline_uuid = Uuid()
    keep_alive_ms = 1_000

    # now we have enough to make our Establish message
    my_name_is_my_passport = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=timestamp,
        keep_alive_ms=keep_alive_ms,
    )

    logging.debug("my_name_is_my_passport: {}".format(my_name_is_my_passport))
    # respond to client that sent the preceding establish message
    est_ack = EstablishmentAck.from_establish(my_name_is_my_passport, server_seq_num=0)
    est_ack = cast(EstablishmentAck, est_ack)
    est_ack_bytes = est_ack.serialize()

    (est_ack_deser, remaining_bytes) = EstablishmentAck.deserialize(data=est_ack_bytes)
    assert est_ack_deser == est_ack
    assert remaining_bytes == bytes()
