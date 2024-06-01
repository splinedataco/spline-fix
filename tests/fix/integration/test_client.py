import pytest
import multiprocessing as mp
from multiprocessing import Pipe
from multiprocessing.connection import Connection
import logging
from typing import cast, Optional, Tuple
import time
from io import BytesIO
import datetime as dt
import polars as pl
from pathlib import Path
import selectors

# used to kill a process when normal methods don't work
import os

from joshua.fix.server import FixServerSession
from joshua.fix.client import FixClientSession
from joshua.fix.common import FixpMessageType
from joshua.fix.fixp.messages import (
    Jwt,
    SPLINE_FIXP_SESSION_SCHEMA_ID,
    NegotiationReject,
)
from joshua.fix.fixp.terminate import Terminate
from joshua.fix.messages import SplineMessageHeader
from joshua.fix.data_dictionary import SPLINE_MUNI_DATA_SCHEMA_ID
from joshua.fix.client import HandleNegotiation, HandleTerminate
from joshua.fix.reader import SessionStateEnum
from joshua.fix.sofh import Sofh

mp_ctx = mp.get_context("fork")
logger = mp_ctx.get_logger()
logger.setLevel(logging.root.level)


def initialize() -> None:
    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def message_pump_once(
    to_from_client: Connection, to_from_server: Connection, delay: float = 0.1
) -> None:
    rsel = selectors.DefaultSelector()
    # wsel = selectors.DefaultSelector()
    rsel.register(
        fileobj=to_from_client, events=selectors.EVENT_READ, data=to_from_server
    )
    rsel.register(
        fileobj=to_from_server, events=selectors.EVENT_READ, data=to_from_client
    )
    """
    wsel = wsel.register(
        fileobj=to_from_client, events=selectors.EVENT_WRITE, data=to_from_server
    )
    wsel = wsel.register(
        fileobj=to_from_server, events=selectors.EVENT_WRITE, data=to_from_client
    )
    """
    events = rsel.select(timeout=delay)
    for key, _mask in events:
        conn = key.fileobj
        other_conn = key.data
        try:
            byte_data = conn.recv_bytes()  # type: ignore[union-attr]
            logging.debug(
                "received: {} bytes and sent to other".format(byte_data.hex(":"))
            )
            other_conn.send_bytes(byte_data)
        except EOFError:
            # sometimes this happens, just move on.
            pass


def make_server_socket() -> Tuple[Connection, Connection]:
    c, s = Pipe(duplex=True)
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


def make_server_client_combo(
    client_send_ms: int = 100,
    server_send_ms: int = 100,
    credentials: Optional[Jwt] = None,
) -> Tuple[Connection, Connection, FixServerSession, FixClientSession]:
    to_from_server, server = make_server(send_ms=server_send_ms)
    to_from_client, client_conn = make_server_socket()

    client = FixClientSession(
        conn=client_conn,
        conn_other_end=to_from_client,
        heartbeat_ms=client_send_ms,
        credentials=credentials,
    )
    return (to_from_client, to_from_server, server, client)


@pytest.mark.timeout(timeout=5, method="signal")
def test_client_handle_negotiation_reject() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_client_handle_negotiation_reject -o log_cli=true
    """
    to_from_client, to_from_server, server, client = make_server_client_combo(
        client_send_ms=500, server_send_ms=500
    )
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
    client_reader = client._readers[0]
    logging.debug("client_reader: {}".format(client_reader))
    logging.debug("server_reader: {}".format(client_reader))
    client_context = client_reader.session_context

    logging.debug("generated bad Jwt. Now call send_negotiate with it.")
    # need this to "prime the pump"
    HandleNegotiation.send_negotiate(
        credentials=tok_gen,
        context=client_context,
    )
    buffer = BytesIO()
    # read from client
    buffer.write(to_from_client.recv_bytes())
    # send to server
    to_from_server.send_bytes(buf=buffer.getvalue())
    buffer.seek(0, 0)
    buffer.truncate(0)

    # server responds with negotiationReject because we don't have permission
    buffer.write(to_from_server.recv_bytes())
    remaining_bytes = buffer.getvalue()
    # store for client later
    buffer_to_client = remaining_bytes
    next_sofh = Sofh.find_sofh(remaining_bytes)
    # should be 0
    assert next_sofh == 0
    (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
    logging.debug("test: sofh: {}".format(sofh))
    # buffer has consumed this data, so we'll overwrite with anything remaining
    # read header and reject
    (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
    assert header.schema_id == SPLINE_FIXP_SESSION_SCHEMA_ID
    assert header.template_id == FixpMessageType.NegotiationReject.value
    (rej, remaining_bytes) = NegotiationReject.deserialize(remaining_bytes)
    logging.debug("test: NegotiationReject: {}".format(rej))
    logging.debug("test: remaining_bytes={!r}".format(remaining_bytes.hex(":")))
    assert remaining_bytes == bytes()
    to_from_client.send_bytes(buffer_to_client)

    # consume message in buffer
    # we've already verified that remaining_bytes == bytes()
    buffer.seek(0, 0)
    buffer.truncate(0)
    # find next sofh
    next_sofh = Sofh.find_sofh(buffer.getvalue())
    # should be None
    assert next_sofh is None
    # server should have terminated
    # buffer.write(to_from_server.recv_bytes())
    # (sofh, remaining) = Sofh.deserialize(buffer.getvalue())
    # and should be a terminate message with reason
    buffer.seek(0, 0)
    if to_from_server.poll(0):
        try:
            buffer.write(to_from_server.recv_bytes())
            assert len(buffer.getvalue()) == 0
        except EOFError:
            logging.debug("got EOFError as expected, continuing")

    server_reader = server._readers[0]
    assert (
        server_reader.session_state == SessionStateEnum.Terminating
        or server_reader.session_state == SessionStateEnum.Terminated
    )

    # failsafe so test doesn't loop forever before failing
    retries = 5
    while (
        client_reader.session_state != SessionStateEnum.Terminated
        and client_reader.session_state != SessionStateEnum.Terminate
        and retries > 0
    ):
        logging.debug(
            "client_reader.session_state = {}".format(client_reader.session_state)
        )
        time.sleep(1)
        retries -= 1
    server.stop_all()
    client.stop_all()
    server._readers[0].session_context.connection.close()
    client._readers[0].session_context.connection.close()
    to_from_client.close()
    to_from_server.close()
    assert retries > 0


@pytest.mark.skip("need to fix currently failing connection reset by peer")
@pytest.mark.timeout(timeout=7, method="signal")
def test_client_send_terminate_after_negotiate_establish() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_client_send_terminate_after_negotiate_establish -o log_cli=true
    """
    to_from_client, to_from_server, server, client = make_server_client_combo(
        client_send_ms=350, server_send_ms=330
    )
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
    client_reader = client._readers[0]
    server_reader = server._readers[0]
    client_context = client_reader.session_context
    logging.debug("test_client: client_context: {}".format(client_context))

    logging.debug("test_client: session_id={}".format(client_context.session_id))

    # need this to "prime the pump"
    HandleNegotiation.send_negotiate(
        credentials=tok_gen,
        context=client_context,
    )
    logging.debug("out of loop check for Establishing and Established")
    # but we may be faster than the processing of a message, so check in a loop with a short delay
    # at 0.1s/loop this is 2 sec
    max_count = 20
    while (
        client_reader.session_state != SessionStateEnum.Established
        or server_reader.session_state != SessionStateEnum.Established
    ):
        message_pump_once(to_from_client, to_from_server)
        logging.debug(f"client_reader.session_state={client_reader.session_state}")
        logging.debug(f"server_reader.session_state={server_reader.session_state}")
        time.sleep(0.1)
        max_count -= 1
        if max_count == 0:
            raise Exception("Test should not take this long")

    # we should be established at this point
    client_reader = client._readers[0]
    assert client_reader.session_state == SessionStateEnum.Established
    server_reader = server._readers[0]
    assert server_reader.session_state == SessionStateEnum.Established

    # the client has received the EstablishmentAck but hasn't had time
    # to process it yet. Give it a moment and then verify that the session
    # is Established
    time.sleep(0.1)
    assert client_reader.session_state == SessionStateEnum.Established

    # now have the client terminate
    (handled, remaining_bytes) = HandleTerminate.send_terminate(
        data=bytes(),
        context=client_reader.session_context,
        termination_str=" Client requested.",
    )
    max_count = 20
    while (
        client_reader.session_state != SessionStateEnum.Terminated
        or server_reader.session_state != SessionStateEnum.Terminated
    ):
        message_pump_once(to_from_client, to_from_server)
        logging.debug(f"client_reader.session_state={client_reader.session_state}")
        logging.debug(f"server_reader.session_state={server_reader.session_state}")
        time.sleep(0.1)
        max_count -= 1
        if max_count == 0:
            raise Exception("Test should not take this long")

    # now, both ends should have closed
    assert server_reader.session_state == SessionStateEnum.Terminated
    assert client_reader.session_state == SessionStateEnum.Terminated
    # these connections should be closed by now but never seem to
    # work out that way, so just skipping for now.
    # time.sleep(5)
    # assert to_from_client.closed
    # assert to_from_server.closed

    server.stop_all()
    client.stop_all()
    server_reader.session_context.connection.close()
    client_reader.session_context.connection.close()
    to_from_client.close()
    to_from_server.close()


# @pytest.mark.timeout(timeout=5, method="signal")
@pytest.mark.skip("pipe found closed by client")
@pytest.mark.timeout(timeout=5)
def test_client_heartbeats_after_negotiate_establish() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_client_heartbeats_after_negotiate_establish -o log_cli=true
    """
    # make them go fast so we can maybe see them in test
    heartbeat_send_ms = 250
    # just to make them different
    heartbeat_recv_ms = 230
    to_from_client, to_from_server, server, client = make_server_client_combo(
        client_send_ms=heartbeat_recv_ms, server_send_ms=heartbeat_send_ms
    )
    logging.debug(
        "both client and server have been created, and should both be running"
    )
    assert to_from_client.writable and to_from_client.readable
    assert to_from_server.writable and to_from_server.writable
    assert not to_from_client.closed and not to_from_server.closed
    logging.debug(
        f"setting both server and client send_hb_interval_ms to {heartbeat_send_ms}"
    )
    server._readers[0].session_context.send_hb_interval_ms = heartbeat_send_ms
    server._readers[0].session_context.recv_hb_interval_ms = heartbeat_recv_ms
    client._readers[0].session_context.send_hb_interval_ms = heartbeat_send_ms
    client._readers[0].session_context.recv_hb_interval_ms = heartbeat_recv_ms
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
    client_reader = client._readers[0]
    server_reader = server._readers[0]
    client_context = client_reader.session_context
    logging.debug("test_client: client_context: {}".format(client_context))

    logging.debug("test_client: session_id={}".format(client_context.session_id))

    logging.debug("have client send_negotiate")
    # need this to "prime the pump"
    HandleNegotiation.send_negotiate(
        credentials=tok_gen,
        context=client_context,
    )
    buffer = BytesIO()
    server_messages = 0
    client_reader = client._readers[0]
    server_reader = server._readers[0]
    client_sent_establish = False
    server_sent_established_ack = False
    time.sleep(0.01)
    while not client_sent_establish or not server_sent_established_ack:
        logging.debug(
            f"client({client_reader.session_context.is_client}) session_state={client_reader.session_state} client_sent_establish={client_sent_establish}"
        )
        logging.debug(
            f"client({server_reader.session_context.is_client}) session_state={server_reader.session_state} server_sent_establishment_ack={server_sent_established_ack}"
        )

        # read from client

        buffer.seek(0, 0)
        buffer.truncate(0)
        # read from client
        logging.debug("poll and read to_from_client")
        if (
            to_from_client.poll(0)
            and not to_from_client.closed
            and to_from_client.readable
        ):
            try:
                buffer.write(to_from_client.recv_bytes())
                logging.debug("from client: {!r}".format(buffer.getvalue().hex(":")))
                # send to server
                logging.debug("send that data on to_from_server")
                to_from_server.send_bytes(buf=buffer.getvalue())
                # check what type of message, should only be unsequenced messages at this point
                (sofh, remaining_bytes) = Sofh.deserialize(buffer.getvalue())
                (header, remaining_bytes) = SplineMessageHeader.deserialize(
                    remaining_bytes
                )
                msg_type = FixpMessageType(header.template_id)
                logging.debug("client sent to server: {}".format(msg_type))
                # could be Negotiate, Establish, UnsequencedHeartbeat
                if msg_type == FixpMessageType.Establish:
                    client_sent_establish = True
            except EOFError:
                continue

        buffer.seek(0, 0)
        buffer.truncate(0)
        logging.debug("poll and read to_from_server")
        if (
            not to_from_server.closed
            and to_from_server.readable
            and to_from_server.poll(0)
        ):
            try:
                buffer.write(to_from_server.recv_bytes())
                logging.debug("from server: {!r}".format(buffer.getvalue().hex(":")))
                sofh_start = Sofh.find_sofh(buffer.getvalue())
                assert sofh_start == 0
                server_messages += 1
                logging.debug("send that data on to_from_client")
                logging.debug("to client: {!r}".format(buffer.getvalue().hex(":")))
                to_from_client.send_bytes(buffer.getvalue())
                (sofh, remaining_bytes) = Sofh.deserialize(buffer.getvalue())
                (header, remaining_bytes) = SplineMessageHeader.deserialize(
                    remaining_bytes
                )
                msg_type = FixpMessageType(header.template_id)
                logging.debug("server sent to client: {}".format(msg_type))
                if msg_type == FixpMessageType.EstablishmentAck:
                    server_sent_established_ack = True
            except EOFError as ex:
                logging.debug(
                    f"got EOFError while trying to read from server. May have closed since the check, continuing. {ex}"
                )
                continue

        buffer.seek(0, 0)
        buffer.truncate(0)
        # 1/10 of a heartbeat
        time.sleep(heartbeat_send_ms / 10_000)

    # we should be established at this point
    logging.debug(
        "out of first loop. assert both sides are established or establishing"
    )

    while (
        client_reader.session_state != SessionStateEnum.Established
        or server_reader.session_state != SessionStateEnum.Established
    ):
        logging.debug(
            f"client({client_reader.session_context.is_client}) session_state={client_reader.session_state}"
        )
        logging.debug(
            f"client({server_reader.session_context.is_client}) session_state={server_reader.session_state}"
        )
        # 1/10 of a heartbeat
        time.sleep(heartbeat_send_ms / 10_000)
    logging.debug("client and server are Established.")

    assert client_reader.session_state == SessionStateEnum.Established
    assert server_reader.session_state == SessionStateEnum.Established
    logging.debug("client and server are established")

    # now established. We will sleep for max_duration_seconds and should see
    # heartbeats. sequence message from server, unsequenced heartbeat from client
    start = dt.datetime.now()
    # duration is well over multiple heartbeat expirations. We want to ensure
    # we're still established at the end
    max_duration_sec = 2
    while (dt.datetime.now() - start).total_seconds() < max_duration_sec:
        logging.debug(
            "looping to read heartbeats total_seconds={:4.3f} < {}".format(
                (dt.datetime.now() - start).total_seconds(), max_duration_sec
            )
        )
        logging.debug("to_from_server.poll(0)")
        # no delay. Will do a shared delay at end
        if to_from_server.poll(0):
            logging.debug("data available from server")
            buffer.seek(0, 0)
            buffer.truncate(0)
            buffer.write(to_from_server.recv_bytes())
            logging.debug("from server: {!r}".format(buffer.getvalue().hex(":")))
            logging.debug("send to client")
            to_from_client.send_bytes(buffer.getvalue())
        logging.debug("to_from_client.poll(0)")
        # no delay. Will do a shared delay at end
        if to_from_client.poll(0):
            logging.debug("data available from client")
            buffer.seek(0, 0)
            buffer.truncate(0)
            buffer.write(to_from_client.recv_bytes())
            logging.debug("from client: {!r}".format(buffer.getvalue().hex(":")))
            # send to server
            to_from_server.send_bytes(buffer.getvalue())
        logging.debug("sleep(0.01) before next poll")
        # 1/10 of a heartbeat
        time.sleep(heartbeat_send_ms / 10_000)

    # check for state here and ensure still in established on both sides.
    assert client_reader.session_state == SessionStateEnum.Established
    assert server_reader.session_state == SessionStateEnum.Established
    logging.debug(
        """client and server are still established.\
        If we made it here, it means that the heartbeats went back and forth."""
    )

    # assert now() - loop start time  > hb interval * 3
    # to have connections close, the test needs to close the appropriate
    # end to simulate the close
    # raise EOFError
    logging.debug("have client send terminate")
    # now have the client terminate
    (handled, remaining_bytes) = HandleTerminate.send_terminate(
        data=bytes(),
        context=client_reader.session_context,
        termination_str=" Client requested.",
    )
    # so the client would have sent a terminate message, read it and send
    # it to the server
    buffer.seek(0, 0)
    buffer.truncate(0)
    if to_from_client.poll(1) and to_from_client.readable:
        logging.debug(
            "client_reader.conn.poll() == True and client_reader.conn.readable == True"
        )
        buffer.write(to_from_client.recv_bytes())
    else:
        raise EOFError
    logging.debug("client terminate: {!r}".format(buffer.getvalue().hex(":")))
    # send to server if server not closed
    if not to_from_server.closed:
        logging.debug("sending clients terminate request to server")
        to_from_server.send_bytes(buffer.getvalue())

    # then the server should send back a termination message
    buffer.seek(0, 0)
    buffer.truncate(0)
    while to_from_server.poll(0) and to_from_server.readable:
        buffer.write(to_from_server.recv_bytes())

        remaining_bytes = buffer.getvalue()
        # send to client to process
        to_from_client.send_bytes(remaining_bytes)
        next_sofh = Sofh.find_sofh(remaining_bytes)
        # should be 0
        assert next_sofh == 0
        (sofh, remaining_bytes) = Sofh.deserialize(remaining_bytes)
        logging.debug("test: sofh: {}".format(sofh))
        # buffer has consumed this data, so we'll overwrite with anything remaining
        # read header and reject
        (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
        if header.template_id == FixpMessageType.Sequence.value:
            time.sleep(0.01)
            continue
        else:
            assert header.schema_id == SPLINE_FIXP_SESSION_SCHEMA_ID
            assert header.template_id == FixpMessageType.Terminate.value
            (rej, remaining_bytes) = Terminate.deserialize(remaining_bytes)
            logging.debug("test: Client received Terminate from server: {}".format(rej))
            logging.debug("test: remaining_bytes={!r}".format(remaining_bytes.hex(":")))
            assert remaining_bytes == bytes()

            break

    check_count = 10
    while (
        server_reader.session_state != SessionStateEnum.Terminated
        or client_reader.session_state != SessionStateEnum.Terminated
    ) and check_count > 0:
        logging.debug("server.session_state={}".format(server_reader.session_state))
        logging.debug("client.session_state={}".format(client_reader.session_state))

        time.sleep(0.1)
        check_count -= 1
    # now, both ends should have closed
    logging.debug("assert both connections are in terminated state")
    assert server_reader.session_state == SessionStateEnum.Terminated
    assert client_reader.session_state == SessionStateEnum.Terminated
    # these connections should be closed by now but never seem to
    # work out that way, so just skipping for now.
    # time.sleep(5)
    # assert to_from_client.closed
    # assert to_from_server.closed

    logging.debug("stop all server readers")
    server.stop_all()
    logging.debug("stop all client readers")
    client.stop_all()
    logging.debug("return")
    server_reader.session_context.connection.close()
    client_reader.session_context.connection.close()
    to_from_client.close()
    to_from_server.close()


# @pytest.mark.timeout(timeout=5, method="signal")
@pytest.mark.skip(
    reason="passes when run singly, and using pytest, but fails when run with tox"
)
@pytest.mark.timeout(timeout=5)
def test_client_heartbeats_fail_after_negotiate_establish() -> None:
    """To run this and get debug logging output
    pytest --capture=tee-sys --log-level=DEBUG -k test_client_heartbeats_fail_after_negotiate_establish -o log_cli=true

    What this should test. Create a client and server. Have the client start the negotiation with the server.
    Pass messages back and forth until both sides are established. Continue forwarding all messages from the client
    to the server, but sink all data from the server. This will simulate an issue with the recv side for the client.
    After 3 missed heartbeat intervals the client should send a Terminate, and then immediately close the connection.
    """
    to_from_client, to_from_server, server, client = make_server_client_combo()
    logging.debug(
        "both client and server have been created, and should both be running"
    )
    assert to_from_client.writable and to_from_client.readable
    assert to_from_server.writable and to_from_server.writable
    assert not to_from_client.closed and not to_from_server.closed
    # make them go fast so we can maybe see them in test
    heartbeat_send_ms = 100
    logging.debug(
        f"setting both server and client send_hb_interval_ms to {heartbeat_send_ms}"
    )
    server._readers[0].session_context.send_hb_interval_ms = heartbeat_send_ms
    client._readers[0].session_context.send_hb_interval_ms = heartbeat_send_ms
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
    client_reader = client._readers[0]
    server_reader = server._readers[0]
    client_context = client_reader.session_context
    logging.debug("test_client: client_context: {}".format(client_context))

    logging.debug("test_client: session_id={}".format(client_context.session_id))

    logging.debug("have client send_negotiate")
    # need this to "prime the pump"
    HandleNegotiation.send_negotiate(
        credentials=tok_gen,
        context=client_context,
    )
    buffer = BytesIO()
    client_reader = client._readers[0]
    server_reader = server._readers[0]
    client_negotiated = False
    server_negotiated = False
    client_established = False
    server_established = False
    client_heartbeated = False
    server_heartbeated = False
    client_terminated = False
    server_terminated = False
    while (
        not client_reader.session_state == SessionStateEnum.Terminated
        or not server_reader.session_state == SessionStateEnum.Terminated
    ):
        logging.debug(
            f"client({client_reader.session_context.is_client}) session_state={client_reader.session_state}"
        )
        logging.debug(
            f"client({server_reader.session_context.is_client}) session_state={server_reader.session_state}"
        )
        if client_reader.session_state != SessionStateEnum.Terminated:
            buffer.seek(0, 0)
            buffer.truncate(0)
            # read from client
            logging.debug("poll and read to_from_client")
            if (
                not to_from_client.closed
                and to_from_client.readable
                and to_from_client.poll(0.01)
            ):
                buffer.write(to_from_client.recv_bytes())
                logging.debug("from client: {!r}".format(buffer.getvalue().hex(":")))
                # always send to server
                logging.debug("send that data on to_from_server")
                to_from_server.send_bytes(buf=buffer.getvalue())
                (seq, remaining_bytes) = Sofh.deserialize(buffer.getvalue())
                (header, remaining_bytes) = SplineMessageHeader.deserialize(
                    remaining_bytes
                )
                msg_type = FixpMessageType(header.template_id)
                match msg_type:
                    case FixpMessageType.Negotiate:
                        assert (
                            client_negotiated is False
                            and client_established is False
                            and client_heartbeated is False
                            and client_terminated is False
                        )
                        client_negotiated = True
                    case FixpMessageType.Establish:
                        assert (
                            client_negotiated is True
                            and client_established is False
                            and client_heartbeated is False
                            and client_terminated is False
                        )
                        client_established = True
                    case FixpMessageType.UnsequencedHeartbeat:
                        assert (
                            client_negotiated is True
                            and client_established is True
                            and client_terminated is False
                        )
                        client_heartbeated = True
                    case FixpMessageType.Terminate:
                        assert (
                            client_negotiated is True
                            and client_established is True
                            and client_terminated is False
                        )
                        client_terminated = True
                        # give some time for both client and server to process
                        time.sleep(heartbeat_send_ms / 1_000 * 3)
                    case _:
                        logging.error(f"client default match. msg_type={msg_type}")

        if server_reader.session_state != SessionStateEnum.Terminated:
            buffer.seek(0, 0)
            buffer.truncate(0)
            logging.debug("poll and read to_from_server")
            if (
                not to_from_server.closed
                and to_from_server.readable
                and to_from_server.poll(0.01)
            ):
                try:
                    buffer.write(to_from_server.recv_bytes())
                except ConnectionResetError as ex:
                    logging.debug(f"server reset connection before read: {ex}")
                    # give enough time for the client to realize server is gone
                    time.sleep(heartbeat_send_ms / 1_000 * 3)
                    # break
                logging.debug("from server: {!r}".format(buffer.getvalue().hex(":")))
                (seq, remaining_bytes) = Sofh.deserialize(buffer.getvalue())
                (header, remaining_bytes) = SplineMessageHeader.deserialize(
                    remaining_bytes
                )
                msg_type = FixpMessageType(header.template_id)
                match msg_type:
                    case FixpMessageType.NegotiationResponse:
                        assert (
                            server_negotiated is False
                            and server_established is False
                            and server_heartbeated is False
                            and server_terminated is False
                        )
                        server_negotiated = True
                        logging.debug("send that data on to_from_client")
                        logging.debug(
                            "to client: {!r}".format(buffer.getvalue().hex(":"))
                        )
                        to_from_client.send_bytes(buffer.getvalue())

                    case FixpMessageType.EstablishmentAck:
                        assert (
                            server_negotiated is True
                            and server_established is False
                            and server_heartbeated is False
                            and server_terminated is False
                        )
                        server_established = True
                        logging.debug("send that data on to_from_client")
                        logging.debug(
                            "to client: {!r}".format(buffer.getvalue().hex(":"))
                        )
                        to_from_client.send_bytes(buffer.getvalue())
                    case FixpMessageType.Sequence:
                        assert (
                            server_negotiated is True
                            and server_established is True
                            and server_terminated is False
                        )
                        server_heartbeated = True
                        # don't forward to client, will look like
                        # it has hung, from the clients point of view
                    case FixpMessageType.Terminate:
                        assert (
                            server_negotiated is True
                            and server_established is True
                            and server_terminated is False
                        )
                        server_terminated = True
                        # give some time for the server to set to terminated
                        time.sleep(heartbeat_send_ms / 1_000 * 3)
                        # don't forward to client, will look like
                        # it has hung, from the clients point of view
                    case _:
                        logging.error(f"server default match. msg_type={msg_type}")

                buffer.seek(0, 0)
                buffer.truncate(0)
            # 1/10 of a heartbeat
            time.sleep(heartbeat_send_ms / 10_000)

    # where are we now?
    logging.debug(f"client_negotiated={client_negotiated}")
    logging.debug(f"server_negotiated={server_negotiated}")
    logging.debug(f"client_established={client_established}")
    logging.debug(f"server_established={server_established}")
    logging.debug(f"client_heartbeated={client_heartbeated}")
    logging.debug(f"server_heartbeated={server_heartbeated}")
    logging.debug(f"client_terminated={client_terminated}")
    logging.debug(f"server_terminated={server_terminated}")

    # we should be established at this point
    logging.debug("out of first loop. assert both sides should be terminated")
    assert client_reader.session_state == SessionStateEnum.Terminated
    assert server_reader.session_state == SessionStateEnum.Terminated

    logging.debug("stop all server readers")
    server.stop_all()
    logging.debug("stop all client readers")
    client.stop_all()
    logging.debug("return")
    server_reader.session_context.connection.close()
    client_reader.session_context.connection.close()
    to_from_client.close()
    to_from_server.close()


def message_pump(client_conn: Connection, server_conn: Connection) -> None:
    to_from_client = client_conn
    to_from_server = server_conn
    buffer = BytesIO()
    while not (to_from_client.closed and to_from_server.closed):
        if (
            not to_from_client.closed
            and to_from_client.readable
            and to_from_client.poll(0.01)
        ):
            buffer.seek(0, 0)
            buffer.truncate()
            buffer.write(to_from_client.recv_bytes())
            logging.debug(
                "message_pump: from client: {!r}".format(buffer.getvalue().hex(":"))
            )
            # always send to server
            logging.debug("send that data on to_from_server")
            if not to_from_server.closed and to_from_server.writable:
                to_from_server.send_bytes(buf=buffer.getvalue())
            else:
                raise ConnectionResetError("message_pump: could not write to server")
            (seq, remaining_bytes) = Sofh.deserialize(buffer.getvalue())

            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            header = cast(SplineMessageHeader, header)
            schema_id = header.schema_id
            msg_type: str
            # TODO: move application template_ids into an Enum
            if schema_id == SPLINE_MUNI_DATA_SCHEMA_ID:
                if header.template_id == 1:
                    msg_type = "MuniCurve"
                elif header.template_id == 2:
                    msg_type = "MuniYieldPrediction"
                else:
                    msg_type = f"Unknown message schema_id={SPLINE_MUNI_DATA_SCHEMA_ID} template_id={header.template_id}"
            elif schema_id == SPLINE_FIXP_SESSION_SCHEMA_ID:
                msg_type = str(FixpMessageType(cast(int, header.template_id)))

            logging.debug(f"message_pump: client sent: {msg_type}")
        if (
            not to_from_server.closed
            and to_from_server.readable
            and to_from_server.poll(0.01)
        ):
            buffer.seek(0, 0)
            buffer.truncate()
            buffer.write(to_from_server.recv_bytes())
            logging.debug(
                "message_pump: from server: {!r}".format(buffer.getvalue().hex(":"))
            )
            logging.debug("message_pump: send that data on to_from_client")
            if not to_from_client.closed and to_from_client.writable:
                to_from_client.send_bytes(buf=buffer.getvalue())
            else:
                raise ConnectionResetError("could not write to client")
            (seq, remaining_bytes) = Sofh.deserialize(buffer.getvalue())
            (header, remaining_bytes) = SplineMessageHeader.deserialize(remaining_bytes)
            schema_id = header.schema_id
            msg_type = ""
            # TODO: move application template_ids into an Enum
            if schema_id == SPLINE_MUNI_DATA_SCHEMA_ID:
                if header.template_id == 1:
                    msg_type = "MuniCurve"
                elif header.template_id == 2:
                    msg_type = "MuniYieldPrediction"
                else:
                    msg_type = f"Unknown message schema_id={SPLINE_MUNI_DATA_SCHEMA_ID} template_id={header.template_id}"
            elif schema_id == SPLINE_FIXP_SESSION_SCHEMA_ID:
                msg_type = str(FixpMessageType(header.template_id))

            logging.debug(f"message_pump: server sent: {msg_type}")


# @pytest.mark.timeout(timeout=20, method="signal")
@pytest.mark.timeout(timeout=20)
def test_client_server_with_app_data(integration_test_data_dir: str) -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_client_server_with_app_data -o log_cli=true
    """
    initialize()
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
    to_from_client, to_from_server, server, client = make_server_client_combo(
        client_send_ms=300, server_send_ms=300, credentials=tok_gen
    )
    logging.debug(
        f"make_server_client_combo returned tfc: {to_from_client} tfs: {to_from_server} s: {server} c: {client}"
    )

    client_reader = client._readers[0]
    client_context = client_reader.session_context
    client_context.credentials = tok_gen
    logging.debug(
        "start. client._readers[0].session_context.credentials={}".format(
            client._readers[0].session_context.credentials
        )
    )
    server_reader = server._readers[0]
    server_context = server_reader.session_context
    logging.debug("test_client: client_context: {}".format(client_context))

    logging.debug("test_client: session_id={}".format(client_context.session_id))

    # need this to "prime the pump"
    client.send_negotiate(
        credentials=tok_gen,
        context=client_context,
    )

    message_pump_proc = mp_ctx.Process(
        target=message_pump,
        kwargs={"client_conn": to_from_client, "server_conn": to_from_server},
        daemon=True,
    )
    message_pump_proc.start()

    itdd = Path(integration_test_data_dir)
    curves_20230511700_dir = itdd / "curves" / "2023" / "05" / "11" / "17" / "00"
    # about 3k rows
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
    yield_predictions_df = pl.read_parquet(preds_20230511700_path)

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

    guess_next_server_send_seq = 0
    try:
        logger.debug("build features")
        for feat in features:
            path = curves_20230511700_dir / feat / "curve.parquet"
            logger.debug("test path is: {}".format(path))
            dfs.append(pl.read_parquet(path))

        while (
            server_context.session_state != SessionStateEnum.Established
            or client_context.session_state != SessionStateEnum.Established
        ):
            time.sleep(0.01)

        assert server_context.session_state == SessionStateEnum.Established
        assert client_context.session_state == SessionStateEnum.Established

        logger.debug("calling MuniCurvesMessageFactory")
        # pass to the factory along with the feature list and a date
        curve_dt_tm = dt.datetime(2023, 5, 11, 17, 00)
        num_curves = 0
        if server_context.curves_permitted():
            num_curves = server.send_curves(
                dt_tm=curve_dt_tm, feature_sets=features, curves=dfs
            )
        num_yields = 0
        if server_context.predictions_permitted():
            yield_dt_tm = dt.datetime(2024, 2, 20, 17, 00)
            num_yields = server.send_yields(
                dt_tm=yield_dt_tm, yields=yield_predictions_df
            )

        # and the total number of messages should == the seq_num
        guess_next_server_send_seq = num_curves + num_yields + 1
        while client_context.recv_next_seq < server_context.send_next_seq:
            time.sleep(0.01)
    except Exception as ex:
        logging.error(f"sending curves and yields failed with: {ex}")
        raise ex
    finally:
        client._readers[0].send_terminate(
            data=bytes(),
            context=client._readers[0].session_context,
            termination_str="test end",
        )
        server.stop_all()
        client.stop_all()

        # using the same guess_next because the server sent the same
        # number that the client received
        logging.debug("assert")
        assert guess_next_server_send_seq == server_context.send_next_seq
        assert guess_next_server_send_seq == client_context.recv_next_seq
    logging.debug("before server.is_alive")
    message_pump_proc.kill()
    message_pump_proc.join()
    message_pump_proc.close()
    if server._readers_proc[0].is_alive():
        logging.debug("before server.terminate()")
        server._readers_proc[0].terminate()
        logging.debug("before server.join()")
        server._readers_proc[0].join()
    logging.debug("before client.is_alive")
    if client._readers_proc[0].is_alive():
        logging.debug("before client.terminate()")
        client._readers_proc[0].terminate()
        logging.debug("before client.join()")
        client._readers_proc[0].join()
    logging.debug("server.before readers_proc.close()")
    server._readers_proc[0].close()
    logging.debug("client.before readers_proc.close()")
    client._readers_proc[0].close()

    logging.debug("before to_from_client.close()")
    to_from_client.close()
    logging.debug("before to_from_server.close()")
    to_from_server.close()
    logging.debug("after to_from_server.close()")

    pytest_pid = os.getpid()
    pytest_pgid = os.getpgid(pytest_pid)
    logging.debug(f"pid={pytest_pid} pgid={pytest_pgid}")
    server_reader.session_context.connection.close()
    client_reader.session_context.connection.close()
    to_from_client.close()
    to_from_server.close()
    # logging.warning(f"kill -SIGTERM {pytest_pid}")
    # subprocess.Popen(["kill", "-SIGTERM", str(pytest_pid)])
    # logging.warning(f"kill -SIGTERM {pytest_pid}")
    # subprocess.Popen(["kill", "-SIGTERM", str(pytest_pid)])

    # logging.warning(f"kill -SIGKILL {pytest_pid}")
    # subprocess.Popen(["kill", "-SIGKILL", str(pytest_pid)])
