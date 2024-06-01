import pytest
import multiprocessing as mp
from multiprocessing.connection import Connection
import logging
from io import BytesIO
import datetime as dt
from typing import cast, Tuple
import time

from joshua.fix.server import FixServerSession
from joshua.fix.fixp.messages import (
    Uuid,
    Jwt,
    Negotiate,
    NegotiationReject,
    NegotiationResponse,
    FlowTypeEnum,
)
from joshua.fix.fixp.establish import (
    Establish,
    EstablishmentRejectCodeEnum,
    EstablishmentAck,
    EstablishmentReject,
    REJECTION_REASON_STRINGS as ESTABLISH_REJECTION_STRINGS,
)
from joshua.fix.fixp.terminate import (
    Terminate,
    TerminationCodeEnum,
)
from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.generate import generate_message_with_header
from joshua.fix.sofh import Sofh

# mp_ctx = mp.get_context("spawn")
mp_ctx = mp.get_context("fork")
logger = mp_ctx.get_logger()
logger.setLevel(logging.root.level)


def get_timestamp() -> float:
    timestamp: float
    while True:
        timestamp = dt.datetime.now().timestamp()
        timestamp_i = int(timestamp * 1_000_000_000)
        timestamp_f = timestamp_i / 1_000_000_000.0
        if timestamp == timestamp_f:
            break
        logger.debug(
            "timestamp={} != {} = reconstituted timestamp".format(
                timestamp, timestamp_f
            )
        )
        time.sleep(1)
    return timestamp


def make_server_socket() -> Tuple[Connection, Connection]:
    c, s = mp_ctx.Pipe(duplex=True)
    return (c, s)


def make_server(send_ms: int = 100) -> Tuple[Connection, FixServerSession]:
    client_conn, server_conn = make_server_socket()
    server = FixServerSession(
        conn=server_conn,
        stop_at_eof=True,
        conn_other_end=client_conn,
        heartbeat_ms=send_ms,
    )
    # we close the server_conn end because we won't be using that end.
    # to communicate with the server we will read/write from the other
    # end client_conn
    # now should be done in reader_start after spawned. (spawn/fork
    # makes a copy of all file descriptors, so we close the other end
    # after that)
    # server_conn.close()
    return (client_conn, server)


@pytest.mark.timeout(timeout=2, method="signal")
def test_server_handle_negotiation_reject() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_negotiation_reject -o log_cli=true
    """
    (client_conn, fix_server) = make_server(send_ms=1000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        # "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
    )
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(0.01) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            assert header.schema_id == NegotiationReject._schema_id
            assert header.template_id == NegotiationReject._messageType.value
            assert header.version == NegotiationReject._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationReject.deserialize(remaining_bytes)
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
        elif client_conn.closed or not client_conn.readable or not client_conn.writable:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")

    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_negotiation_response() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_negotiation_response -o log_cli=true
    """
    (client_conn, fix_server) = make_server(10_000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            logger.debug("{}".format(header))
            assert header.schema_id == NegotiationResponse._schema_id
            assert header.template_id == NegotiationResponse._messageType.value
            assert header.version == NegotiationResponse._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
            logger.debug("join server")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")

    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_establish_ack() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_establish_ack -o log_cli=true
    """
    (client_conn, fix_server) = make_server(10_000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            assert header.schema_id == NegotiationResponse._schema_id
            assert header.template_id == NegotiationResponse._messageType.value
            assert header.version == NegotiationResponse._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
            logger.debug("join server")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")
        # assert False

    # same thing but we're going to respond to the response with an establish message
    spline_timestamp = get_timestamp()
    establish = Establish(
        credentials=None,
        uuid=spline_uuid,
        timestamp=spline_timestamp,
        keep_alive_ms=1234,
        next_sequence_num=1,
    )
    establish_bytes = generate_message_with_header(
        block_length=establish._blockLength,
        template_id=establish._messageType.value,
        schema_id=establish._schemaId,
        version=establish._version,
        # credentials
        num_groups=1,
        data=establish.serialize(),
    )
    client_conn.send_bytes(establish_bytes)
    logger.debug(f"client(establish)->server: {len(negotiate_bytes)} bytes")
    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (_sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            logger.debug("MessageHeader: {}".format(header))
            logger.debug(
                f"schema_id: got: {header.schema_id} expect: {EstablishmentAck._schemaId}"
            )
            assert header.schema_id == EstablishmentAck._schemaId
            assert header.template_id == EstablishmentAck._messageType.value
            assert header.version == EstablishmentAck._version
            assert header.num_groups == 1
            (est_ack, remaining_bytes) = EstablishmentAck.deserialize(remaining_bytes)
            assert remaining_bytes == bytes()
            logger.debug("client received EstablishmentAck:\n{}".format(est_ack))
            logger.debug("join server")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")
    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_establish_reject_bad_session_id() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_establish_reject_bad_session_id -o log_cli=true
    """
    (client_conn, fix_server) = make_server(send_ms=10_000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            assert header.schema_id == NegotiationResponse._schema_id
            assert header.template_id == NegotiationResponse._messageType.value
            assert header.version == NegotiationResponse._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
            logger.debug("join server")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")
        # assert False

    # make a new uuid. Since it won't match the one used to negotiate
    # the session, it should get rejected.
    uuid2 = Uuid()
    spline_timestamp = get_timestamp()
    establish = Establish(
        credentials=None,
        uuid=uuid2,
        timestamp=spline_timestamp,
        keep_alive_ms=1234,
        next_sequence_num=1,
    )
    establish_bytes = generate_message_with_header(
        block_length=establish._blockLength,
        template_id=establish._messageType.value,
        schema_id=establish._schemaId,
        version=establish._version,
        # credentials
        num_groups=1,
        data=establish.serialize(),
    )
    client_conn.send_bytes(establish_bytes)
    logger.debug(f"client(establish)->server: {len(establish_bytes)} bytes")

    # read back the reject
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (_sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            logger.debug("MessageHeader: {}".format(header))
            logger.debug(
                f"schema_id: got: {header.schema_id} expect: {EstablishmentReject._schemaId}"
            )
            assert header.schema_id == EstablishmentReject._schemaId
            assert header.template_id == EstablishmentReject._messageType.value
            assert header.version == EstablishmentReject._version
            assert header.num_groups == 0
            (est_rej, remaining_bytes) = EstablishmentReject.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received EstablishmentReject:\n{}".format(est_rej))
            est_rej = cast(EstablishmentReject, est_rej)
            assert (
                est_rej.reject_code.value
                == EstablishmentRejectCodeEnum.Unspecified.value
            )
            assert est_rej._reject_str.data.decode("latin1").startswith("Unspecified:")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")

    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_establish_reject_not_negotiated() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_establish_reject_not_negotiated -o log_cli=true
    """
    (client_conn, fix_server) = make_server(10000)
    # make a new uuid. Since it won't match the one used to negotiate
    # the session, it should get rejected.
    uuid2 = Uuid()
    spline_timestamp = get_timestamp()
    establish = Establish(
        credentials=None,
        uuid=uuid2,
        timestamp=spline_timestamp,
        keep_alive_ms=1234,
        next_sequence_num=1,
    )
    establish_bytes = generate_message_with_header(
        block_length=establish._blockLength,
        template_id=establish._messageType.value,
        schema_id=establish._schemaId,
        version=establish._version,
        # credentials
        num_groups=1,
        data=establish.serialize(),
    )
    client_conn.send_bytes(establish_bytes)
    logger.debug(f"client(establish)->server: {len(establish_bytes)} bytes")

    # read back the reject
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (_sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            logger.debug("MessageHeader: {}".format(header))
            logger.debug(
                f"schema_id: got: {header.schema_id} expect: {EstablishmentReject._schemaId}"
            )
            assert header.schema_id == EstablishmentReject._schemaId
            assert header.template_id == EstablishmentReject._messageType.value
            assert header.version == EstablishmentReject._version
            assert header.num_groups == 0
            (est_rej, remaining_bytes) = EstablishmentReject.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received EstablishmentReject:\n{}".format(est_rej))
            assert est_rej.reject_code == EstablishmentRejectCodeEnum.Unnegotiated
            assert (
                est_rej._reject_str.data.decode("latin1")
                == ESTABLISH_REJECTION_STRINGS[est_rej.reject_code.value]
            )

        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")

    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_establish_reject_already_established() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_establish_reject_already_established -o log_cli=true
    """
    (client_conn, fix_server) = make_server(send_ms=10_000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            assert header.schema_id == NegotiationResponse._schema_id
            assert header.template_id == NegotiationResponse._messageType.value
            assert header.version == NegotiationResponse._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
            logger.debug("join server")
        elif client_conn.closed:
            logger.debug("connection closed")
    except EOFError:
        logger.debug("client received EOF from server")
        # assert False

    # same thing but we're going to respond to the response with an establish message
    for i in range(0, 2):
        spline_timestamp = get_timestamp()
        establish = Establish(
            credentials=None,
            uuid=spline_uuid,
            timestamp=spline_timestamp,
            keep_alive_ms=1234,
            next_sequence_num=1,
        )
        establish_bytes = generate_message_with_header(
            block_length=establish._blockLength,
            template_id=establish._messageType.value,
            schema_id=establish._schemaId,
            version=establish._version,
            # credentials
            num_groups=1,
            data=establish.serialize(),
        )
        client_conn.send_bytes(establish_bytes)
        logger.debug(f"client(establish)->server: {len(negotiate_bytes)} bytes")
        # read back the response
        read_buf = BytesIO()
        logger.debug("client check if data to read")
        try:
            if client_conn.poll(1) is True:
                logger.debug("client recv_bytes")
                read_buf.write(client_conn.recv_bytes())
                logger.debug("client: {!r}".format(read_buf.getvalue()))
                # server should do stuff and print
                # client should have received a response from this
                data = read_buf.getvalue()
                (_sofh, remaining_bytes) = Sofh.deserialize(data)
                # then header
                (header, remaining_bytes) = SplineMessageHeader.deserialize(
                    remaining_bytes
                )
                header = cast(SplineMessageHeader, header)
                logger.debug("MessageHeader: {}".format(header))
                logger.debug(
                    f"schema_id: got: {header.schema_id} expect: {EstablishmentAck._schemaId}"
                )
                if i == 0:
                    assert header.schema_id == EstablishmentAck._schemaId
                    assert header.template_id == EstablishmentAck._messageType.value
                    assert header.version == EstablishmentAck._version
                    assert header.num_groups == 1
                    (est_ack, remaining_bytes) = EstablishmentAck.deserialize(
                        remaining_bytes
                    )
                    assert remaining_bytes == bytes()
                    logger.debug(
                        "client received EstablishmentAck:\n{}".format(est_ack)
                    )
                    logger.debug("join server")
                else:
                    assert header.schema_id == EstablishmentReject._schemaId
                    assert header.template_id == EstablishmentReject._messageType.value
                    assert header.version == EstablishmentReject._version
                    assert header.num_groups == 0
                    (est_rej, remaining_bytes) = EstablishmentReject.deserialize(
                        remaining_bytes
                    )
                    assert remaining_bytes == bytes()
                    logger.debug(
                        "client received EstablishmentReject:\n{}".format(est_rej)
                    )
                    assert (
                        est_rej.reject_code
                        == EstablishmentRejectCodeEnum.AlreadyEstablished
                    )
                    assert (
                        est_rej._reject_str.data.decode("latin1")
                        == ESTABLISH_REJECTION_STRINGS[est_rej.reject_code.value]
                    )
            elif client_conn.closed:
                logger.debug("connection closed")
                raise EOFError
        except EOFError:
            logger.debug("client received EOF from server")
    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()


@pytest.mark.timeout(timeout=3, method="signal")
def test_server_handle_client_terminate() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_server_handle_client_terminate_after_negotiate -o log_cli=true
    """
    # need to negotiate
    (client_conn, fix_server) = make_server(send_ms=1000)
    # create the negotiate message
    roles = ["Coverage:Full", "User:Admin", "User:Business"]
    capabilities = [
        "TodaysCurves",
        "AllYieldFeatures",
        "RealTimeData",
        "YieldRevSectors",
        "HistoricCurves",
        "UserAdmin",
        "AllYieldCoupons",
        "AclDataApi",
        "PricePredictions",
    ]
    jwtdata = {"name": "Thaddeus Covert"}
    issuer = "Lambda Auth API"
    subject = "thaddeus@splinedata.com"
    tok_gen = Jwt.generate(
        roles=roles,
        capabilities=capabilities,
        issuer=issuer,
        userid=subject,
        username=jwtdata["name"],
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
    # create a header for the message
    negotiate_bytes = generate_message_with_header(
        block_length=sending_someone_to._block_length,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
        data=sending_someone_to.serialize(),
    )
    client_conn.send_bytes(negotiate_bytes)
    logger.debug(f"client->server: {len(negotiate_bytes)} bytes")

    client_conn.poll(3)
    est_buf = BytesIO()
    est_buf.write(client_conn.recv_bytes())
    logger.debug("client: {!r}".format(est_buf.getvalue()))
    # server should do stuff and print
    # client should have received a response from this
    data = est_buf.getvalue()
    (sofh, remaining_bytes) = Sofh.deserialize(data)
    # then header
    (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
    header = cast(SplineMessageHeader, header)
    logger.debug("{}".format(header))
    assert header.schema_id == NegotiationResponse._schema_id
    assert header.template_id == NegotiationResponse._messageType.value
    assert header.version == NegotiationResponse._version
    assert header.num_groups == 1
    (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(remaining_bytes)
    assert remaining_bytes == bytes()
    # then we'll immediately send a terminate message.
    terminate = Terminate(spline_uuid, termination_code=TerminationCodeEnum.Finished)
    logger.debug("terminate message after create: {}".format(terminate))
    logger.debug(
        "terminate._block_length: {} blockLength: {} block_length: {}".format(
            terminate._block_length, terminate.blockLength, terminate.block_length
        )
    )

    # read back the response
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    try:
        if client_conn.poll(1) is True:
            # putting this here because it means the server has had time to send
            # back a message
            # create a header for the message
            logger.debug("terminate message: {}".format(terminate))
            term_bytes = generate_message_with_header(
                block_length=terminate.block_length,
                template_id=terminate.template_id,
                schema_id=terminate.schema_id,
                version=terminate.version,
                num_groups=terminate.num_groups,
                data=terminate.serialize(),
            )

            client_conn.send_bytes(term_bytes)
            logger.debug(f"client->server: {len(term_bytes)} bytes")

            logger.debug("client recv_bytes")
            read_buf.write(client_conn.recv_bytes())
            logger.debug("client: {!r}".format(read_buf.getvalue()))
            # server should do stuff and print
            # client should have received a response from this
            data = read_buf.getvalue()
            (sofh, remaining_bytes) = Sofh.deserialize(data)
            # then header
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            logger.debug("{}".format(header))
            assert header.schema_id == NegotiationResponse._schema_id
            assert header.template_id == NegotiationResponse._messageType.value
            assert header.version == NegotiationResponse._version
            assert header.num_groups == 1
            (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(
                remaining_bytes
            )
            assert remaining_bytes == bytes()
            logger.debug("client received:\n{}".format(neg_resp))
            logger.debug(
                "len(remaining_bytes)={} remaining_bytes={!r}".format(
                    len(remaining_bytes), remaining_bytes.hex(":")
                )
            )
            assert len(remaining_bytes) > 0

        elif client_conn.closed:
            logger.debug("connection closed")
            raise EOFError
    except EOFError:
        logger.debug("client received EOF from server")
    except OSError:
        logger.debug("client tried reading from closed socket")

    logger.debug("client close connection")
    client_conn.close()
    logger.debug("reader finished")
    fix_server.stop_all()
