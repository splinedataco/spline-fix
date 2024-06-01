from typing import cast
from joshua.fix.types import (
    SplineByteData,
    SplineNanoTime,
)
import pytest
from joshua.fix.common import FixpMessageType
from joshua.fix.fixp.messages import (
    NegotiationReject,
    NegotiationRejectCodeEnum,
    Uuid,
    Negotiate,
    NegotiationResponse,
    Jwt,
    FlowTypeEnum,
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


def test_uuid_create_default() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_uuid_create_default -o log_cli=true
    """
    # things we can check is that the 4th section starts with a 4
    # that there are 5 parts
    # and that they are all in the correct range.
    # since the constructor checks that when reused, we'll use that here.
    for _ in range(0, 10):
        uuid = Uuid()
        logging.debug("default uuid is: {}".format(uuid))
        uuid_str = str(uuid)
        uuid_dup = Uuid(uuid_str)
        # they should be the same, and shouldn't throw
        assert uuid == uuid_dup


def test_uuid_create_copy_fail() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_uuid_create_copy_fail -o log_cli=true
    """
    uuid = Uuid()
    logging.debug("default uuid is: {}".format(uuid))
    # make it the wrong version
    bad_uuid = str(uuid)
    p0, p1, p2, p3, p4 = bad_uuid.split("-")
    p2 = "5" + p2[1:]
    with pytest.raises(ValueError) as exinfo:
        Uuid("-".join([p0, p1, p2, p3, p4]))
    # they should be the same, and shouldn't throw
    assert str(exinfo.value) == "Uuid provided for reuse must be version 4."

    # put an invalid character into each field
    for i in range(0, 5):
        bad_uuid = str(uuid)
        p0, p1, p2, p3, p4 = str(bad_uuid).split("-")
        if i == 0:
            p0 = "z" + p0[1:]
        elif i == 1:
            p1 = p1[0:i] + "Q" + p1[i + 1 :]
        elif i == 2:
            p2 = p2[0:i] + "g" + p2[i + 1 :]
        elif i == 3:
            p3 = p3[0:i] + "R" + p3[i + 1 :]
        elif i == 4:
            p4 = p4[0:i] + "x" + p4[i + 1 :]
        with pytest.raises(ValueError) as exinfo:
            Uuid("-".join([p0, p1, p2, p3, p4]))
        assert str(exinfo.value) == "Uuid may only contain hexidecimal values. 0-9A-F"

        p0, p1, p2, p3, p4 = str(bad_uuid).split("-")
        p4 = p4[0:i] + "-" + p4[i + 1 :]
        with pytest.raises(ValueError) as exinfo:
            Uuid("-".join([p0, p1, p2, p3, p4]))
        assert str(exinfo.value) == "too many values to unpack (expected 5)"


def test_uuid_serde() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_uuid_serde -o log_cli=true
    """
    test_uuid = Uuid()
    test_uuid_b = test_uuid.serialize()
    test_uuid_d, remaining_bytes = Uuid.deserialize(test_uuid_b)
    assert remaining_bytes == bytes()
    assert test_uuid_d == test_uuid


def test_fixp_negotiate_create() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_negotiate_create -o log_cli=true
    """
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )
    tok_gen = cast(Jwt, tok_gen)
    timestamp = dt.datetime.now().timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    # now we have enough to make our Negotiate message
    sending_someone_to = Negotiate(
        credentials=tok_gen,
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )
    logging.debug("Negotiate msg:\n{}".format(sending_someone_to))
    assert sending_someone_to._sessionId == spline_uuid
    assert sending_someone_to._messageType == FixpMessageType.Negotiate
    assert sending_someone_to._credentials == tok_gen
    assert sending_someone_to.timestamp_nano == SplineNanoTime(timestamp)
    assert sending_someone_to._clientFlow == FlowTypeEnum.NONE


def test_fixp_jwt_generate_create() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_jwt_generate_create -o log_cli=true
    """
    """
    logging.debug(
        "env secret_key={!r}".format(
            base64.urlsafe_b64decode(os.getenv("SPLINE_SECRET_KEY", "bad") + "==")
        )
    )
    secret_key = base64.urlsafe_b64decode(
        os.getenv("SPLINE_SECRET_KEY", default=secrets.token_urlsafe(32)) + "=="
    )
    logging.debug("env_secret_key={!r}".format(secret_key))
    """
    default_jwt = Jwt()
    logging.debug("{}".format(default_jwt))
    assert default_jwt == default_jwt

    """
    # this requires the production token to be in the environment variable to work
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJyb2xlcyI6WyJDb3ZlcmFnZTpGdWxsIiwiVXNlcjpBZG1pbiIsIlVzZXI6QnVzaW5lc3MiXSwiY2FwYWJpbGl0aWVzIjpbIlRvZGF5c0N1cnZlcyIsIkFsbFlpZWxkRmVhdHVyZXMiLCJSZWFsVGltZURhdGEiLCJBbGxZaWVsZENvdXBvbnMiLCJZaWVsZFJldlNlY3RvcnMiLCJIaXN0b3JpY0N1cnZlcyIsIlVzZXJBZG1pbiIsIlRvZGF5c0N1cnZlcyIsIkFsbFlpZWxkRmVhdHVyZXMiLCJBbGxZaWVsZENvdXBvbnMiLCJZaWVsZFJldlNlY3RvcnMiLCJSZWFsVGltZURhdGEiLCJBY2xEYXRhQXBpIiwiUHJpY2VQcmVkaWN0aW9ucyIsIkhpc3RvcmljQ3VydmVzIiwiQWNsRGF0YUFwaSIsIlRvZGF5c0N1cnZlcyIsIkhpc3RvcmljQ3VydmVzIl0sImRhdGEiOnsibmFtZSI6IlRoYWRkZXVzIENvdmVydCJ9LCJpYXQiOjE3MTIyNTYxOTMsImV4cCI6MTcxMjI5MzIwMCwiaXNzIjoiTGFtYmRhIEF1dGggQVBJIiwic3ViIjoidGhhZGRldXNAc3BsaW5lZGF0YS5jb20ifQ.OaXHHitNPSyxCG_bYDlRc631fbHdzeNBNzbLShUyZns"
    real_token = Jwt(token=token)
    logging.debug("real_token: {}".format(real_token))
    """

    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )

    tok_gen_ser = tok_gen.serialize()
    logging.debug("tok_gen_ser: {!r}".format(tok_gen_ser.hex(":")))
    (tok_gen_deser, remaining_bytes) = Jwt.deserialize(tok_gen_ser)
    assert remaining_bytes == bytes()
    assert tok_gen == tok_gen_deser


def test_fixp_messages_negotiate_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_negotiate_serialize -o log_cli=true
    """
    # from the client point of view
    # we need to negotiate a connection and identify ourselves
    # we'll identify by getting a jwt from our login server first
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )
    tok_gen = cast(Jwt, tok_gen)

    tok_gen_ser = tok_gen.serialize()
    (tok_gen_deser, _) = Jwt.deserialize(tok_gen_ser)
    assert tok_gen_deser == tok_gen

    # we'll make a timestamp so we can test, if not given will be created
    # for us

    timestamp = get_timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    # now we have enough to make our Negotiate message
    sending_someone_to_negotiate = Negotiate(
        credentials=tok_gen,
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )

    logging.debug("sending_to_negotiate: {}".format(sending_someone_to_negotiate))
    someone_ser = sending_someone_to_negotiate.serialize()

    # manually decode
    remaining_bytes = someone_ser
    # should be sessionId/uuid
    (uuid_deser, extra) = Uuid.deserialize(remaining_bytes[0:16])
    assert extra == bytes()
    assert uuid_deser == spline_uuid
    # then nanotime
    (nano_deser, extra) = SplineNanoTime.deserialize(remaining_bytes[16:24])
    assert extra == bytes()
    assert nano_deser.timestamp == timestamp
    # just 1 byte for clientFlow
    (cf_int_deser,) = struct.unpack("<B", remaining_bytes[24:25])
    cf_deser = FlowTypeEnum(cf_int_deser)
    assert extra == bytes()
    assert cf_deser == client_flow
    # padding of 3, should be 0
    for i in range(0, 3):
        b = remaining_bytes[25 + i : 25 + 1 + i]
        assert b == b"\x00"
    (jwt_deser, remaining_bytes) = Jwt.deserialize(remaining_bytes[28:])
    assert remaining_bytes == bytes()
    assert jwt_deser == tok_gen
    logging.debug("jwt_deser: {}".format(jwt_deser))

    # now try all together
    (someone_deser, remaining_bytes) = Negotiate.deserialize(someone_ser)
    assert remaining_bytes == bytes()
    assert (
        someone_deser == sending_someone_to_negotiate
    ), "where'd he learn to negotiate like that?"


def test_fixp_messages_negotiationresponse_create() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_messages_negotiationresponse_create -o log_cli=true
    """
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )
    tok_gen = cast(Jwt, tok_gen)
    timestamp = get_timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    # now we have enough to make our Negotiate message
    sending_someone_to = Negotiate(
        credentials=tok_gen,
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )
    logging.debug("Negotiate msg:\n{}".format(sending_someone_to))
    assert sending_someone_to._sessionId == spline_uuid
    assert sending_someone_to._messageType == FixpMessageType.Negotiate
    assert sending_someone_to._credentials == tok_gen
    assert sending_someone_to.timestamp_nano == SplineNanoTime(timestamp)
    assert sending_someone_to._clientFlow == FlowTypeEnum.NONE

    neg_rsp = NegotiationResponse.from_negotiate(sending_someone_to)
    assert neg_rsp.sessionId == sending_someone_to.sessionId
    assert neg_rsp._messageType == FixpMessageType.NegotiationResponse
    assert neg_rsp._credentials.data == bytes()
    assert neg_rsp._serverFlow == FlowTypeEnum.IDEMPOTENT
    assert neg_rsp._block_length == sending_someone_to._block_length
    assert neg_rsp._group_pad == sending_someone_to._group_pad
    assert neg_rsp._request_timestamp_nano == sending_someone_to.timestamp_nano


def test_fixp_messages_negotiationresponse_serialize() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_negotiationresponse_serialize -o log_cli=true
    """
    # from the client point of view
    # we need to negotiate a connection and identify ourselves
    # we'll identify by getting a jwt from our login server first
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )
    tok_gen = cast(Jwt, tok_gen)

    tok_gen_ser = tok_gen.serialize()
    (tok_gen_deser, _) = Jwt.deserialize(tok_gen_ser)
    assert tok_gen_deser == tok_gen

    # we'll make a timestamp so we can test, if not given will be created
    # for us
    # why? because sometimes we get a timestamp that doesn't
    # work well with floation point and ends up differing. So
    # we'll compare until we get one that does work so we won't
    # assert later
    timestamp = get_timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    # now we have enough to make our Negotiate message
    sending_someone_to_negotiate = Negotiate(
        credentials=tok_gen,
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )

    logging.debug("sending_to_negotiate: {}".format(sending_someone_to_negotiate))

    # from the server point of view, receives a negotiate request
    meat_popsicle = NegotiationResponse.from_negotiate(sending_someone_to_negotiate)
    meat_popsicle = cast(NegotiationResponse, meat_popsicle)
    mp_ser = meat_popsicle.serialize()

    remaining_bytes = mp_ser
    # should be sessionId/uuid
    (uuid_deser, extra) = Uuid.deserialize(remaining_bytes[0:16])
    assert extra == bytes()
    assert uuid_deser == spline_uuid
    # then nanotime
    (nano_deser, extra) = SplineNanoTime.deserialize(remaining_bytes[16:24])
    nano_deser = cast(SplineNanoTime, nano_deser)
    assert extra == bytes()
    assert nano_deser.timestamp == timestamp
    # just 1 byte for serverFlow
    (sf_int_deser,) = struct.unpack("<B", remaining_bytes[24:25])
    sf_deser = FlowTypeEnum(sf_int_deser)
    assert extra == bytes()
    assert sf_deser == FlowTypeEnum.IDEMPOTENT
    # padding of 3, should be 0
    for i in range(0, 3):
        b = remaining_bytes[25 + i : 25 + 1 + i]
        assert b == b"\x00"
    # for now, this should be empty
    (data_deser, remaining_bytes) = SplineByteData.deserialize(
        remaining_bytes[28:], bytes
    )
    assert remaining_bytes == bytes()
    assert data_deser == bytes()

    # now try all together
    (meat_popsicle_deser, remaining_bytes) = NegotiationResponse.deserialize(mp_ser)
    assert remaining_bytes == bytes()
    assert (
        meat_popsicle_deser == meat_popsicle
    ), "where'd he learn to negotiate like that?"


def test_fixp_messages_negotiationreject_create_serde() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_messages_negotiationreject_create_serde -o log_cli=true
    """
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "AllYieldCoupons",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "TodaysCurves",
        "AllYieldFeatures",
        "AllYieldCoupons",
        "YieldRevSectors",
        "RealTimeData",
        "AclDataApi",
        "PricePredictions",
        "HistoricCurves",
        "AclDataApi",
        "TodaysCurves",
        "HistoricCurves",
    ]
    data = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=data["name"],
    )
    tok_gen = cast(Jwt, tok_gen)
    timestamp = get_timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    # now we have enough to make our Negotiate message
    sending_someone_to = Negotiate(
        credentials=tok_gen,
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )

    spline_timestamp = SplineNanoTime(other=timestamp)
    # after the server has received the negotiate message, it
    # could send the negotiationreject message

    reasons = [NegotiationRejectCodeEnum(x) for x in range(0, 4)]
    for reject_reason in reasons:
        neg_rej = NegotiationReject.from_negotiate(
            sending_someone_to, reject_code=reject_reason
        )
        neg_rej = cast(NegotiationReject, neg_rej)
        logging.debug("NegotiationReject msg:\n{}".format(neg_rej))
        # serialize it to put on wire (still would need sofh and header)
        neg_rej_ser = neg_rej.serialize()

        remaining_bytes = neg_rej_ser
        # should be sessionId/uuid
        (uuid_deser, extra) = Uuid.deserialize(remaining_bytes[0:16])
        assert extra == bytes()
        assert uuid_deser == spline_uuid
        # then nanotime
        (nano_deser, extra) = SplineNanoTime.deserialize(remaining_bytes[16:24])
        assert extra == bytes()
        assert nano_deser == spline_timestamp
        # just 1 byte for serverFlow
        (rc_int_deser,) = struct.unpack("<B", remaining_bytes[24:25])
        rc_deser = NegotiationRejectCodeEnum(rc_int_deser)
        assert extra == bytes()
        assert rc_deser == reject_reason
        # padding of 3, should be 0
        for i in range(0, 3):
            b = remaining_bytes[25 + i : 25 + 1 + i]
            assert b == b"\x00"
        # for now, this should be empty
        (data_deser, remaining_bytes) = SplineByteData.deserialize(
            remaining_bytes[28:], bytes
        )
        assert remaining_bytes == bytes()
        data_bytes = (REJECTION_REASON_STRINGS[rc_int_deser] + "").encode("latin-1")
        assert data_deser == data_bytes

        # now try all together
        (neg_rej_deser, remaining_bytes) = NegotiationReject.deserialize(neg_rej_ser)
        assert remaining_bytes == bytes()
        assert neg_rej_deser == neg_rej, "where'd he learn to negotiate like that?"

        assert neg_rej_deser._sessionId == spline_uuid
        assert neg_rej_deser._messageType == FixpMessageType.NegotiationReject
        assert neg_rej_deser.request_timestamp_nano == SplineNanoTime(timestamp)
        assert neg_rej_deser._reject_code == reject_reason
        unspecified_reject_str = (
            REJECTION_REASON_STRINGS[neg_rej_deser._reject_code.value] + ""
        )
        assert neg_rej_deser._reject_str == SplineByteData(
            data=unspecified_reject_str.encode("latin-1")
        )
