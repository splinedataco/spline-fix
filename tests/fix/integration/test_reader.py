import pytest
from io import BytesIO
from typing import cast, Optional, Tuple
import logging
from multiprocessing.connection import Connection
import multiprocessing as mp

from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory
from joshua.fix.reader import (
    SplineFixReader,
    combined_message_id,
    combined_message_tuple,
    SessionStateEnum,
    SessionContext,
)
from joshua.fix.types import SplineNanoTime
import polars as pl
import datetime as dt
from pathlib import Path
from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.fixp.messages import (
    Jwt,
    Uuid,
    FlowTypeEnum,
    Negotiate,
    NegotiationResponse,
    NegotiationReject,
)
from joshua.fix.sofh import Sofh
from joshua.fix.common import align_by
from joshua.fix.generate import generate_message_with_header

from joshua.fix.fixp.messages import NegotiationRejectCodeEnum
from joshua.fix.fixp.terminate import TerminationCodeEnum

# mp_ctx = mp.get_context("spawn")
mp_ctx = mp.get_context("fork")
logger = mp_ctx.get_logger()
logger.setLevel(logging.root.level)


def initialize() -> None:
    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def noop_send_terminate(
    data: bytes,
    context: SessionContext,
    termination_code: Optional[TerminationCodeEnum] = TerminationCodeEnum.Unspecified,
    termination_str: Optional[str] = None,
) -> Tuple[bool, bytes]:
    logging.debug(
        f"""sent noop_send_terminate({data.hex(":")}, {context}, {termination_code}, {termination_str})"""
    )
    return (True, data)


def curves_print_handler(data: bytes, _conn: Connection) -> Tuple[bool, bytes]:
    # because we don't have access to the header, we aren't aware of alignment
    # and because the outside loop doesn't know what we've registered, it
    # doesn't know to to either.
    # TODO:
    # Consider a template handler/base class that has all necessary information
    # in it as getters as well as the visit() method
    # then when registered, the main loop can determine this information from
    # the registered handler.
    remaining_bytes = data
    head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
    head_pad = align_by(head_len, 4)
    remaining_bytes = remaining_bytes[head_pad:]
    (
        dttm_de,
        feature_sets_de,
        dfs_de,
        remaining_bytes,
    ) = MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(remaining_bytes)
    for (
        feat,
        df,
    ) in zip(feature_sets_de, dfs_de):
        logger.debug("curve {}:\n{}".format(feat, df))

    return (True, remaining_bytes)


def yield_predictions_print_handler(
    data: bytes, _conn: Connection
) -> Tuple[bool, bytes]:
    # because we don't have access to the header, we aren't aware of alignment
    # and because the outside loop doesn't know what we've registered, it
    # doesn't know to to either.
    # TODO:
    # Consider a template handler/base class that has all necessary information
    # in it as getters as well as the visit() method
    # then when registered, the main loop can determine this information from
    # the registered handler.
    remaining_bytes = data
    head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
    head_pad = align_by(head_len, 4)
    remaining_bytes = remaining_bytes[head_pad:]
    (
        dttm_de,
        df_de,
        remaining_bytes,
    ) = MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
        remaining_bytes
    )
    logger.debug("prediction {}:\n{}".format(dttm_de, df_de))

    return (True, remaining_bytes)


@pytest.mark.timeout(timeout=20, method="signal")
def test_fix_reader_callback(integration_test_data_dir) -> None:  # type: ignore[no-untyped-def]
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fix_reader_callback -o log_cli=true
    """
    initialize()
    logger.debug("test_fix_reader_callback begin")
    itdd = Path(integration_test_data_dir)
    curves_20230511700_dir = itdd / "curves" / "2023" / "05" / "11" / "17" / "00"
    # 120 rows
    preds_20230511700_path = (
        itdd / "short" / "combined" / "predictions" / "predictions.parquet"
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

    logger.debug("build features")
    for feat in features:
        path = curves_20230511700_dir / feat / "curve.parquet"
        logger.debug("test path is: {}".format(path))
        dfs.append(pl.read_parquet(path))

    logger.debug("calling MuniCurvesMessageFactory")
    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2023, 5, 11, 17, 00)
    bb_c = b"".join(
        [
            c
            for c in MuniCurvesMessageFactory.serialize_from_dataframes(
                dt_tm=dt_tm, feature_sets=features, dfs=dfs
            )
        ]
    )

    bs = BytesIO(bb_c)
    dt_tm = dt.datetime(2024, 2, 20, 17, 00)
    for bb_y in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
        dt_tm, yield_predictions_df
    ):
        bs.write(bb_y)

    reader = SplineFixReader()

    logger.debug("register curve print handler")
    reader.register_handler(
        MuniCurvesMessageFactory.template_id,
        curves_print_handler,
        MuniCurvesMessageFactory.schema_id,
        MuniCurvesMessageFactory.version,
    )
    logger.debug("register yield prediction print handler")
    reader.register_handler(
        MuniYieldPredictionMessageFactory.template_id,
        yield_predictions_print_handler,
        MuniYieldPredictionMessageFactory.schema_id,
        MuniYieldPredictionMessageFactory.version,
    )

    # don't want to deal with Terminate here
    reader.register_send_terminate(noop_send_terminate)
    logger.debug("register byte stream")
    bs.seek(0, 0)
    client_conn, server_conn = mp_ctx.Pipe()
    context = SessionContext(conn=server_conn)
    context.other_end_connection = client_conn
    logger.debug(f"type(server_conn): {type(server_conn)}")
    logger.debug(
        f"isinstance(server_conn, _io.BytesIO): {isinstance(server_conn, BytesIO)}"
    )
    logger.debug("call reader.start()")
    sp = mp_ctx.Process(
        target=reader.reader_start,
        name="test_reader",
        kwargs={"stop_at_eof": True},
        daemon=False,
    )
    sp.start()
    # send data to server
    logger.debug("client send_bytes")
    client_conn.send_bytes(bs.getbuffer())
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    if client_conn.poll(0.1) is True:
        logger.debug("client recv_bytes")
        read_buf.write(client_conn.recv_bytes())
        logger.debug("client: {!r}".format(read_buf.getvalue()))
    elif client_conn.closed:
        logger.debug("connection closed")

    logger.debug("client close connection")
    client_conn.close()
    # server should do stuff and print

    logger.debug("join server")
    sp.join()
    logger.debug("reader finished")


@pytest.mark.timeout(timeout=20, method="signal")
def test_fix_reader_callback_interlaced(integration_test_data_dir) -> None:  # type: ignore[no-untyped-def]
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fix_reader_callback -o log_cli=true
    """
    initialize()
    # mp_ctx = mp.get_context("spawn")
    mp_ctx = mp.get_context("fork")
    logger.debug("test_fix_reader_callback begin")
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

    logger.debug("build features")
    for feat in features:
        path = curves_20230511700_dir / feat / "curve.parquet"
        logger.debug("test path is: {}".format(path))
        dfs.append(pl.read_parquet(path))

    logger.debug("calling MuniCurvesMessageFactory")
    # pass to the factory along with the feature list and a date
    dt_tm = dt.datetime(2023, 5, 11, 17, 00)
    bb_c = b"".join(
        [
            c
            for c in MuniCurvesMessageFactory.serialize_from_dataframes(
                dt_tm=dt_tm, feature_sets=features, dfs=dfs
            )
        ]
    )

    bs = BytesIO(bb_c)
    dt_tm = dt.datetime(2024, 2, 20, 17, 00)
    for bb_y in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
        dt_tm, yield_predictions_df
    ):
        bs.write(bb_y)
        # write another copy of the curves
        bs.write(bb_c)

    reader = SplineFixReader()

    logger.debug("register curve print handler")
    reader.register_handler(
        MuniCurvesMessageFactory.template_id,
        curves_print_handler,
        MuniCurvesMessageFactory.schema_id,
        MuniCurvesMessageFactory.version,
    )
    logger.debug("register yield prediction print handler")
    reader.register_handler(
        MuniYieldPredictionMessageFactory.template_id,
        yield_predictions_print_handler,
        MuniYieldPredictionMessageFactory.schema_id,
        MuniYieldPredictionMessageFactory.version,
    )

    # don't want to deal with Terminate here
    reader.register_send_terminate(noop_send_terminate)

    logger.debug("register byte stream")
    bs.seek(0, 0)
    client_conn, server_conn = mp_ctx.Pipe()
    context = SessionContext(conn=server_conn)
    context.other_end_connection = client_conn
    reader.session_context = context
    sp = mp_ctx.Process(
        target=reader.reader_start,
        name="test_reader",
        kwargs={"stop_at_eof": True},
        daemon=False,
    )
    logger.debug("call reader.start()")
    sp.start()
    # send data to server
    logger.debug("client send_bytes")
    client_conn.send_bytes(bs.getbuffer())
    read_buf = BytesIO()
    logger.debug("client check if data to read")
    if client_conn.poll(0.01) is True:
        logger.debug("client recv_bytes")
        read_buf.write(client_conn.recv_bytes())
        logger.debug("client: {!r}".format(read_buf.getvalue()))
    elif client_conn.closed:
        logger.debug("connection closed")

    logger.debug("client close connection")
    server_conn.close()
    client_conn.close()
    logger.debug("closed connections now terminate daemon thread")

    # sp.terminate()
    # sp.kill()
    # sp.close()
    # client_conn.close()
    # server should do stuff and print
    context.cancel_timer(context._recv_hb_timer)
    context.cancel_timer(context._send_hb_timer)

    logger.debug("reader finished")

    """NOTE: for some manual configuration. Load the dataframe for predictions and get the shape.
    [ins] In [168]: short_pred_lf = pl.scan_parquet("integration_test_data/combined/predictions/2024/02/20/17/00/predictions.parquet")

    [ins] In [169]: short_pred_lf.collect().shape
    Out[169]: (3166, 5)


    Then figure out how many of them were sent.
    # the escape'\' (and) makes flake8 barf, so putting 2 in.
    ❯ rg -e "prediction .*:" -A1 /tmp/test_munipredictions.log | rg  "shape" | sed -Ee 's/.*\\(([[:digit:]]{1,2}), .*\\)$/\1/' | gawk '{sum+=$1} END {print sum}'
    3166

    Matches. Be sure to use the correct file to verify.
    """


@pytest.mark.timeout(timeout=2, method="signal")
def test_combined_message_id_and_tuple() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_combined_message_id_and_tuple -o log_cli=true
    """
    template_ids = [t for t in range(0, 256, 1)]
    schema_ids = [s for s in range(255, -1, -1)]
    versions = [v // 256 for v in range(0, 1024, 4)]

    for t, s, v in zip(template_ids, schema_ids, versions):
        cmi = combined_message_id(t, s, v)
        tt, ts, tv = combined_message_tuple(cmi)

        assert tt == t
        assert ts == s
        assert tv == v


def fixp_reader_negotiate_handler(
    data: bytes, context: SessionContext
) -> Tuple[bool, bytes]:
    """This would be the servers point of view, receiving a negotiate message"""
    conn = context.connection
    remaining_bytes = data
    head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
    head_pad = align_by(head_len, 4)
    remaining_bytes = remaining_bytes[head_pad:]
    try:
        (negotiate_de, remaining_bytes) = Negotiate.deserialize(remaining_bytes)
        negotiate_de = cast(Negotiate, negotiate_de)
    except ValueError as ex:
        logger.warning("Rejected Negotiation attempt: {}".format(str(ex)))
        # since we didn't actually create a negotiate message, we'll
        # get what parts we can
        (parts, remaining_bytes) = Negotiate.deserialize_to_dict(remaining_bytes)
        session_id = parts.get("sessionId")
        session_id = session_id if session_id is not None else Uuid()
        request_timestamp = parts.get("timestamp")
        request_timestamp = (
            request_timestamp if request_timestamp is not None else SplineNanoTime()
        )
        neg_rej = NegotiationReject(
            session_id=session_id,
            request_timestamp=request_timestamp,
            reject_code=NegotiationRejectCodeEnum.Unspecified,
            reject_str=str(ex),
        )
        neg_rej = cast(NegotiationReject, neg_rej)
        neg_rej_head = generate_message_with_header(
            block_length=NegotiationReject._block_length,
            template_id=NegotiationReject._messageType.value,
            schema_id=NegotiationReject._schema_id,
            version=NegotiationReject._version,
            # jwt
            num_groups=1,
        )
        bs = BytesIO()
        bs.write(neg_rej_head)
        bs.write(neg_rej.serialize())
        sofh = Sofh(size=bs.tell(), size_includes_self=True)
        bs.seek(0, 0)
        bs.write(sofh.serialize())
        logger.debug("negotiation attempt failed returning reject: {}".format(neg_rej))
        conn.send_bytes(bs.getbuffer())
    negotiate_de = cast(Negotiate, negotiate_de)
    # here we have a negotiation message, but don't know to allow it or note
    # TODO: verify that sessionid is unique for day.
    logger.debug(f"proffered session id: {negotiate_de._sessionId}")
    # since we have a jwt, they are authenticated and have a valid jwt
    # We need to see what feeds to turn on for the
    # TODO: register messages with reader for feeds/messages client is authorized to receive
    # NOTE: we want to defer the registration until after the session is set up
    # or we may send data before then.
    logger.debug("Got negotiation attempt: {}".format(negotiate_de))
    # here we would validate the login attempt and validate feeds by acl
    jwt = negotiate_de.credentials
    if jwt.curves_permitted and jwt.predictions_permitted:
        neg_resp = NegotiationResponse(
            session_id=negotiate_de.sessionId,
            request_timestamp=negotiate_de.timestamp,
        )
        neg_msg = generate_message_with_header(
            block_length=NegotiationResponse._block_length,
            template_id=NegotiationResponse._messageType.value,
            schema_id=NegotiationResponse._schema_id,
            version=NegotiationResponse._version,
            # empty credentials group
            num_groups=1,
            data=neg_resp.serialize(),
        )
        conn.send_bytes(neg_msg)
    else:
        neg_rej = NegotiationReject.from_negotiate(
            cast(Negotiate, negotiate_de),
            reject_code=NegotiationRejectCodeEnum.Credentials,
            reject_str="Credentials supplied insufficient to grant access.",
        )
        neg_rej = cast(NegotiationReject, neg_rej)

        neg_rej_head_bytes = generate_message_with_header(
            block_length=NegotiationReject._block_length,
            template_id=NegotiationReject._messageType.value,
            schema_id=NegotiationReject._schema_id,
            version=NegotiationReject._version,
            # reject str
            num_groups=1,
            data=neg_rej.serialize(),
        )
        logger.debug("negotiation attempt failed returning reject: {}".format(neg_rej))
        conn.send_bytes(neg_rej_head_bytes)

    return (True, remaining_bytes)


@pytest.mark.timeout(timeout=2, method="signal")
def test_fixp_reader_negotiate_session() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_reader_negotiate_session -o log_cli=true
    """
    logger.debug("test_fix_reader_callback begin")
    # set up the server
    reader = SplineFixReader()
    # server doesn't need to register the other negotiate messages, because
    # it's only expecting the negotiate message.
    # the client will need the negotiate reject and response
    # handlers though.
    logger.debug("register negotiate handler")
    reader.register_handler(
        Negotiate._messageType.value,
        fixp_reader_negotiate_handler,
        Negotiate._schemaId,
        Negotiate._version,
    )
    client_conn, server_conn = mp_ctx.Pipe()
    context = SessionContext(conn=server_conn)
    context.other_end_connection = client_conn
    reader.session_context = context
    sp = mp_ctx.Process(
        target=reader.reader_start,
        name="test_reader",
        kwargs={"stop_at_eof": True},
        daemon=False,
    )
    sp.start()
    server_conn.close()

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
    timestamp = dt.datetime.now().timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    bs = BytesIO()
    # now we have enough to make our Negotiate message
    sending_someone_to = Negotiate(
        credentials=cast(Jwt, tok_gen),
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )
    # create a header for the message
    neg_head = generate_message_with_header(
        block_length=sending_someone_to.blockLength,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
    )
    bs.write(neg_head)
    bs.write(sending_someone_to.serialize())
    # now, fix up the SOFH length
    sofh = Sofh(bs.tell(), size_includes_self=True)
    bs.seek(0, 0)
    bs.write(sofh.serialize())
    client_conn.send_bytes(bs.getvalue())
    logger.debug(f"client->server: {len(bs.getvalue())} bytes")

    read_buf = BytesIO()
    logger.debug("client check if data to read")
    if client_conn.poll(1) is True:
        logger.debug("client recv_bytes")
        read_buf.write(client_conn.recv_bytes())
        logger.debug("client: {!r}".format(read_buf.getvalue()))
    elif client_conn.closed:
        logger.debug("connection closed")

    logger.debug("client close connection")
    client_conn.close()
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
    (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(remaining_bytes)
    assert remaining_bytes == bytes()
    logger.debug("client received:\n{}".format(neg_resp))
    context = reader.session_context
    context.cancel_timer(context._recv_hb_timer)
    context.cancel_timer(context._send_hb_timer)
    logger.debug("join server")
    sp.join()
    logger.debug("reader finished")


@pytest.mark.timeout(timeout=2, method="signal")
def test_fixp_reader_negotiate_session_reject() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_fixp_reader_negotiate_session_reject -o log_cli=true
    """
    logger.debug("test_fix_reader_callback begin")
    # set up the server
    reader = SplineFixReader()
    # server doesn't need to register the other negotiate messages, because
    # it's only expecting the negotiate message.
    # the client will need the negotiate reject and response
    # handlers though.
    logger.debug("register negotiate handler")
    reader.register_handler(
        Negotiate._messageType.value,
        fixp_reader_negotiate_handler,
        Negotiate._schemaId,
        Negotiate._version,
    )
    client_conn, server_conn = mp_ctx.Pipe()
    context = SessionContext(conn=server_conn)
    context.other_end_connection = client_conn
    reader.session_context = context
    sp = mp_ctx.Process(
        target=reader.reader_start, name="test_reader", kwargs={"stop_at_eof": True}
    )
    sp.start()
    server_conn.close()

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
    timestamp = dt.datetime.now().timestamp()
    spline_uuid = Uuid()
    client_flow = FlowTypeEnum.NONE

    bs = BytesIO()
    # now we have enough to make our Negotiate message
    sending_someone_to = Negotiate(
        credentials=cast(Jwt, tok_gen),
        uuid=spline_uuid,
        timestamp=timestamp,
        client_flow=client_flow,
    )
    # create a header for the message
    neg_head = generate_message_with_header(
        block_length=sending_someone_to.blockLength,
        template_id=Negotiate._messageType.value,
        schema_id=Negotiate._schemaId,
        version=Negotiate._version,
        # jwt
        num_groups=1,
    )
    bs.write(neg_head)
    bs.write(sending_someone_to.serialize())
    # now, fix up the SOFH length
    sofh = Sofh(bs.tell(), size_includes_self=True)
    bs.seek(0, 0)
    bs.write(sofh.serialize())
    client_conn.send_bytes(bs.getvalue())
    logger.debug(f"client->server: {len(bs.getvalue())} bytes")

    read_buf = BytesIO()
    logger.debug("client check if data to read")
    if client_conn.poll(1) is True:
        logger.debug("client recv_bytes")
        read_buf.write(client_conn.recv_bytes())
        logger.debug("client: {!r}".format(read_buf.getvalue()))
    elif client_conn.closed:
        logger.debug("connection closed")

    logger.debug("client close connection")
    client_conn.close()
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
    logger.debug("join server")
    context = reader.session_context
    context.cancel_timer(context._recv_hb_timer)
    context.cancel_timer(context._send_hb_timer)
    sp.join()
    logger.debug("reader finished")


@pytest.mark.timeout(timeout=2, method="signal")
def test_server_session_context() -> None:
    c, s = mp_ctx.Pipe()
    session_ctx_0 = SessionContext(c, is_client=False)
    session_ctx_1 = SessionContext(s, is_client=True)

    assert session_ctx_0.recv_next_seq == session_ctx_1.send_next_seq
    assert session_ctx_0.send_next_seq == session_ctx_1.recv_next_seq
    assert session_ctx_0.send_next_seq == 1
    assert session_ctx_0.send_next_seq_increment() == 1
    assert session_ctx_0.send_seq == 1
    assert session_ctx_0.send_next_seq == 2

    sse3 = SessionStateEnum(3)
    session_ctx_0.session_state = sse3
    assert session_ctx_0.session_state.value == 3
    assert session_ctx_0.recv_seq == 0
    assert session_ctx_0.recv_next_seq == 1
    assert session_ctx_0.recv_next_seq == 1
    with pytest.raises(ValueError) as exinfo:
        session_ctx_0.recv_next_seq_increment()
    assert (
        str(exinfo.value)
        == "Sequenced messages on a None type Flow is a protocol error. Terminate connection."
    )
    # still 1
    assert session_ctx_0.recv_next_seq == 1
    assert session_ctx_1.recv_next_seq_increment()
    assert session_ctx_1.recv_next_seq == 2
    session_ctx_0.cancel_timer(session_ctx_0._recv_hb_timer)
    session_ctx_0.cancel_timer(session_ctx_0._send_hb_timer)
    session_ctx_1.cancel_timer(session_ctx_1._recv_hb_timer)
    session_ctx_1.cancel_timer(session_ctx_1._send_hb_timer)
