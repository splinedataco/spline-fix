from typing import cast, List, Optional, Tuple
import multiprocessing as mp
from multiprocessing.connection import Connection
import polars as pl
import datetime as dt

from joshua.fix.common import align_by
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory
from joshua.fix.reader import SplineFixReader, SessionStateEnum, SessionContext
from joshua.fix.fixp.messages import Jwt, Negotiate
from joshua.fix.sofh import Sofh
from joshua.fix.messages import (
    SplineMessageHeader,
)

from io import BytesIO
import logging

from joshua.fix.fixp.messages import (
    FlowTypeEnum,
    NegotiationReject,
    NegotiationResponse,
    Uuid,
)
from joshua.fix.fixp.establish import (
    Establish,
    EstablishmentReject,
    EstablishmentAck,
)
from joshua.fix.fixp.terminate import (
    Terminate,
    TerminationCodeEnum,
    TERMINATE_REASON_STRINGS,
)
from joshua.fix.types import SplineFixType, SplineNanoTime
from joshua.fix.generate import generate_message_with_header
from joshua.fix.common import get_timestamp

# mp_ctx = mp.get_context("spawn")
# mp_ctx = mp.get_context("fork")

start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# logger = mp_ctx.get_logger()
logger = logging.getLogger()
# logger.setLevel(logging.root.level)
logger.setLevel(logging.INFO)


def generate_message_from_type(
    msg: SplineFixType, num_groups: Optional[int] = None
) -> bytes:
    """Given the type of the to the server object (needs the following
    cls.blockLength, cls.template_id, cls.to the server schema_id, cls.versioni
    TODO: add num_groups to session objects
    )
    the data to append
    """
    buffer = BytesIO()
    numg: int
    if num_groups is None:
        try:
            numg = msg.num_groups
        except AttributeError:
            numg = 0
    else:
        numg = int(num_groups)
    neg_msg_bytes = generate_message_with_header(
        block_length=msg.blockLength,
        template_id=msg.template_id,
        schema_id=msg.schema_id,
        version=msg.version,
        # empty credentials group
        num_groups=numg,
        data=msg.serialize(),
    )
    buffer.write(neg_msg_bytes)
    return buffer.getvalue()


class HandleNegotiation:
    @classmethod
    def send_negotiate(
        cls,
        credentials: Jwt,
        context: SessionContext,
        session_id: Optional[Uuid] = None,
    ) -> None:
        # make locals for the data we need
        tok_gen = credentials
        context.credentials = credentials
        if session_id is None:
            session_id = Uuid()
        spline_uuid = session_id
        timestamp = SplineNanoTime(get_timestamp())
        client_flow = FlowTypeEnum.NONE
        # client_conn = context.connection

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
        if not context.connection.closed and context.connection.writable:
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(negotiate_bytes)
                context.set_send_time_last_message(None)
                # logger.debug(f"cs: {len(negotiate_bytes)} bytes")
                context.session_state = SessionStateEnum.Negotiating
            finally:
                context.connection_lock.release()
        else:
            logger.warning(
                f"HandleNegotiation: is_client{context.is_client} tried to send Negotiate message but found socket closed. Terminating."
            )
            context.session_state = SessionStateEnum.Terminate

    @classmethod
    def process_negotiate(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        raise NotImplementedError

    @classmethod
    def process_reject(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        # deserialize
        # reconnect()/not possible with unnamed Pipe, so just close and exit
        # logger.warning("process_reject: {!r}\n{}".format(data, context))
        remaining_bytes = data
        (neg_rej, remaining_bytes) = NegotiationReject.deserialize(remaining_bytes)
        neg_rej = cast(NegotiationReject, neg_rej)
        logger.warning("got reject: {}".format(neg_rej))
        context.session_state = SessionStateEnum.Terminate

        return (True, remaining_bytes)

    @classmethod
    def process_response(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        logger.debug("Process Negotiation Response")
        logger.debug("context: {}".format(context))
        # deserialize
        remaining_bytes = data
        (neg_resp, remaining_bytes) = NegotiationResponse.deserialize(remaining_bytes)
        neg_resp = cast(NegotiationResponse, neg_resp)
        context.session_id = neg_resp.session_id
        context.session_state = SessionStateEnum.Negotiated
        logger.debug("got response: {}".format(neg_resp))
        # send establish.
        return HandleEstablishment.send_establish(data=remaining_bytes, context=context)


class HandleEstablishment:
    @classmethod
    def send_establish(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        # assemble establish from context
        if context.session_id is None:
            context.session_id = Uuid()
        logger.debug("client.send_establish() context={}".format(context))
        spline_timestamp = get_timestamp()
        establish = Establish(
            credentials=None,
            uuid=context.session_id,
            timestamp=spline_timestamp,
            keep_alive_ms=context.send_hb_interval_ms,
            next_sequence_num=context.recv_next_seq,
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
        if not context.connection.closed and context.connection.writable:
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(establish_bytes)
                context.set_send_time_last_message(None)
                context.session_state = SessionStateEnum.Establishing
            finally:
                context.connection_lock.release()
            return (True, data)
        else:
            logger.error(
                f"client({context.is_client}) failed connection.closed == True"
            )
            context.session_state = SessionStateEnum.Terminate
            return (False, data)

    @classmethod
    def process_reject(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        # deserialize
        # Depending on reject reason, we can try to establish
        # again, but only thing we could really fix would be to
        # regenerate the uuid and try again: if other things are
        # preventing establish, we should terminate
        remaining_bytes = data
        (est_rej, remaining_bytes) = EstablishmentReject.deserialize(remaining_bytes)
        est_rej = cast(EstablishmentReject, est_rej)
        # establishment should have worked, so we'll terminate
        return HandleTerminate.send_terminate(
            data=remaining_bytes,
            context=context,
            termination_code=TerminationCodeEnum.Unspecified,
            termination_str=" Establish attempt rejected because {}".format(
                est_rej.reject_str
            ),
        )

    @classmethod
    def process_ack(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        # deserialize
        remaining_bytes = data
        (est_ack, remaining_bytes) = EstablishmentAck.deserialize(remaining_bytes)
        context.session_state = SessionStateEnum.Established
        context.recv_hb_interval_ms = est_ack.keep_alive
        logger.debug("client: EstablishmentAck: {}".format(est_ack))
        logger.debug("client.context.session_state = {}".format(context.session_state))
        return (True, remaining_bytes)


class HandleTerminate:
    @staticmethod
    def send_terminate(
        data: bytes,
        context: SessionContext,
        termination_code: Optional[
            TerminationCodeEnum
        ] = TerminationCodeEnum.Unspecified,
        termination_str: Optional[str] = None,
    ) -> Tuple[bool, bytes]:
        if (
            context.session_state == SessionStateEnum.WaitingOnConnect
            or context.session_state == SessionStateEnum.Terminating
            or context.session_state == SessionStateEnum.Terminated
        ):
            HandleTerminate.terminate_session(
                context, termination_code, termination_str
            )
            return (True, data)

        logger.debug("client.send_terminate called")
        # populate the fields we will need
        session_id: Uuid
        if context.session_id is not None:
            session_id = context.session_id
        else:
            # we don't have even a session_id. Just close the connection.
            logger.debug("we don't think we have a session_id! closing connection")
            context.connection.close()
            context.session_state = SessionStateEnum.Terminated
            session_id = Uuid()

        term_code: TerminationCodeEnum
        term_str: Optional[str]
        if termination_code is not None:
            term_code = termination_code
        else:
            term_code = TerminationCodeEnum.Unspecified

        term_str = TERMINATE_REASON_STRINGS[term_code.value]
        if termination_str is not None:
            term_str += termination_str

        term: Terminate
        term_bytes: bytes

        term = cast(
            Terminate,
            Terminate(
                session_id=session_id,
                termination_code=term_code,
                termination_str=term_str,
            ),
        )
        logger.debug("generating terminate message bytes from type")
        term_bytes = generate_message_from_type(msg=term, num_groups=term.num_groups)
        logger.info("Terminate Session: {}".format(term))
        if not context.connection.closed and context.connection.writable:
            logger.debug("client sending terminate to server")
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(term_bytes)
            except BrokenPipeError as ex:
                logger.debug(
                    f"broken pipe error trying to send terminate to server. {ex}"
                )
                context.session_state = SessionStateEnum.Terminated
            finally:
                context.connection_lock.release()
            context.set_send_time_last_message(None)
            logger.debug(
                "client={} send sent terminate to server: {!r}".format(
                    context.is_client, term_bytes
                )
            )
            logger.debug(
                "context.connection={} closed={} writable={}".format(
                    context.connection,
                    context.connection.closed,
                    context.connection.writable,
                )
            )
        else:
            logger.warning("client connection was closed, didn't send anything")
        context.session_state = SessionStateEnum.Terminating

        return (True, data)

    @staticmethod
    def terminate_session(
        context: SessionContext,
        termination_code: Optional[
            TerminationCodeEnum
        ] = TerminationCodeEnum.Unspecified,
        termination_str: Optional[str] = None,
    ) -> None:
        """Don't send any further messages, just close the connection
        termination_code and termination string here are only used for
        logger.
        """
        logger.warning("Terminating session: {}".format(context))
        if not context.connection.closed:
            logger.warning("client.terminate_session closing connection.")
            context.connection.close()
        context.session_state = SessionStateEnum.Terminated
        context.cancel_timer(context._recv_hb_timer)
        context.cancel_timer(context._send_hb_timer)

    @classmethod
    def process_terminate(
        cls, data: bytes, context: SessionContext
    ) -> Tuple[bool, bytes]:
        logger.debug("client.process_terminate called")
        # sofh and header already consumed. And we've been called because we received a terminate
        remaining_bytes = data
        (terminate_msg, remaining_bytes) = Terminate.deserialize(remaining_bytes)
        logger.debug("process_terminate check session_state")
        if context.session_state == SessionStateEnum.Terminating:
            logger.debug("session_state == {}".format(context.session_state))
            # then we've received our response
            context.session_state = SessionStateEnum.Terminated
            if not context.connection.closed:
                logger.debug("client.process_terminate closed connection")
                context.connection.close()
        else:
            logger.debug(
                "session_state wasn't  Terminating == {}".format(context.session_state)
            )
            # we need to send one back
            # based on the spec, "Upon receiving a Terminate message, the receiver must
            # respond with a Terminate message. The Terminate response must be the last message sent."
            return cls.send_terminate(remaining_bytes, context=context)
        return (True, data)


class FixClientSession:

    def get_unique_uuid(self) -> Uuid:
        return Uuid()

    def __init__(
        self,
        conn: Connection,
        stop_at_eof: bool = False,
        conn_other_end: Optional[Connection] = None,
        heartbeat_ms: int = 1_000,
        credentials: Optional[Jwt] = None,
    ) -> None:
        """Server_conn is only added in case it's necessary/desired
        to close the server connection from the client after spawn
        """
        # self.logger.setLevel(logging.root.level)
        # self.logger.setLevel(logging.DEBUG)
        # self.logger.setLevel(logging.getLogger().level)
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.logger = mp_ctx.get_logger()
        self.logger.setLevel(logging.INFO)

        self._readers = []
        self._readers_proc = []
        reader = SplineFixReader(is_client=True, initial_heartbeat_ms=heartbeat_ms)
        reader.register_byte_stream(conn)

        # register handlers here first
        # set negotiate messages.
        if credentials:
            reader.session_context.credentials = credentials
        session_context = reader.session_context
        if (
            heartbeat_ms >= Establish._keep_alive_min_max[0]
            and heartbeat_ms <= Establish._keep_alive_min_max[1]
        ):
            session_context.send_hb_interval_ms = heartbeat_ms
        else:
            raise ValueError(
                "heartbeat must be in range [{}:{}]".format(
                    Establish._keep_alive_min_max[0], Establish._keep_alive_min_max[1]
                )
            )

        if conn_other_end is not None:
            # only needed for pipes because the client and server
            # filedescriptors are both in the test/caller and so
            # the client has to close it's copy of the server
            # file descriptor so as to be notified of EOF
            session_context.other_end_connection = conn_other_end
        reader.session_context = session_context
        reader = self.register_negotiation_response_handler(reader=reader)
        reader = self.register_negotiation_reject_handler(reader=reader)
        reader = self.register_establishment_ack_handler(reader=reader)
        reader = self.register_establishment_reject_handler(reader=reader)
        reader = self.register_termination_handler(reader=reader)
        reader = self.register_send_terminate(reader=reader)
        reader = self.register_curve_handler(reader=reader)
        reader = self.register_yield_handler(reader=reader)

        sp = mp_ctx.Process(
            target=reader.reader_start,
            name="fix_client",
            kwargs={"stop_at_eof": stop_at_eof},
            daemon=True,
        )
        sp.start()
        self._readers_proc.append(sp)
        self._readers.append(reader)

    def start(self) -> None:
        self.logger.debug("client start")
        for r in self._readers:
            self.logger.debug("client start reader r: {}".format(r))
            r.reader_start()
            self.logger.debug("client started reader")

    def stop_all(self) -> None:
        # TODO:
        # for every reader, terminate and then join
        for rp, r in zip(self._readers_proc, self._readers):
            context = r.session_context
            context.cancel_timer(context._recv_hb_timer)
            context.cancel_timer(context._send_hb_timer)
            self.logger.error("rp.join() being called")
            rp.join(0.1)
            if rp.is_alive():
                self.logger.error("rp.terminate() being called")
                rp.terminate()
                self.logger.error("rp.join() being called")
                rp.join(0.1)
            self.logger.error("called rp.join()")
            try:
                if not r.session_context.connection.closed:
                    r.session_context.connection.close()
            except IOError as ex:
                self.logger.debug(f"server connection already closed. {ex}")
            self.logger.warning("stopped all")

        self.logger.error("stopped all")

    def register_negotiate_handler(self, reader: SplineFixReader) -> SplineFixReader:
        """NOTE: Client shouldn't be getting these.
        When initiating a new connection this state will be active.
        """
        reader.register_handler(
            Negotiate._messageType.value,
            HandleNegotiation.process_negotiate,
            Negotiate._schemaId,
            Negotiate._version,
        )
        reader.session_state = SessionStateEnum.WaitingOnConnect
        return reader

    def register_negotiation_reject_handler(
        self, reader: SplineFixReader
    ) -> SplineFixReader:
        reader.register_handler(
            NegotiationReject._messageType.value,
            HandleNegotiation.process_reject,
            NegotiationReject._schema_id,
            NegotiationReject._version,
        )
        return reader

    def register_negotiation_response_handler(
        self, reader: SplineFixReader
    ) -> SplineFixReader:
        reader.register_handler(
            NegotiationResponse._messageType.value,
            HandleNegotiation.process_response,
            NegotiationResponse._schema_id,
            NegotiationResponse._version,
        )
        return reader

    def register_establishment_reject_handler(
        self, reader: SplineFixReader
    ) -> SplineFixReader:
        reader.register_handler(
            EstablishmentReject._messageType.value,
            HandleEstablishment.process_reject,
            EstablishmentReject._schemaId,
            EstablishmentReject._version,
        )
        return reader

    def register_establishment_ack_handler(
        self, reader: SplineFixReader
    ) -> SplineFixReader:
        reader.register_handler(
            EstablishmentAck._messageType.value,
            HandleEstablishment.process_ack,
            EstablishmentAck._schemaId,
            EstablishmentAck._version,
        )
        return reader

    def register_termination_handler(self, reader: SplineFixReader) -> SplineFixReader:
        reader.register_handler(
            Terminate._messageType.value,
            HandleTerminate.process_terminate,
            Terminate._schemaId,
            Terminate._version,
        )
        return reader

    def register_send_terminate(self, reader: SplineFixReader) -> SplineFixReader:
        reader.register_send_terminate(fn=HandleTerminate.send_terminate)
        return reader

    def register_curve_handler(self, reader: SplineFixReader) -> SplineFixReader:
        reader.register_handler(
            MuniCurvesMessageFactory.template_id,
            HandleApplication.process_curves,
            MuniCurvesMessageFactory.schema_id,
            MuniCurvesMessageFactory.version,
        )
        return reader

    def register_yield_handler(self, reader: SplineFixReader) -> SplineFixReader:
        reader.register_handler(
            MuniYieldPredictionMessageFactory.template_id,
            HandleApplication.process_yields,
            MuniYieldPredictionMessageFactory.schema_id,
            MuniYieldPredictionMessageFactory.version,
        )
        return reader

    def send_negotiate(
        self,
        credentials: Optional[Jwt] = None,
        context: Optional[SessionContext] = None,
        session_id: Optional[Uuid] = None,
    ) -> None:
        if context is not None:
            self._readers[0].session_context = context
        if credentials is not None:
            self._readers[0].session_context.credentials = credentials
        if session_id is not None:
            self._readers[0].session_context.session_id = session_id
        context = self._readers[0].session_context
        return HandleNegotiation.send_negotiate(
            credentials=context.credentials,
            context=context,
            session_id=context.session_id,
        )


class HandleApplication:
    @classmethod
    def curves_action(
        cls,
        dt_tm: dt.datetime,
        feature_sets: List[str],
        curves_dfs: List[pl.DataFrame],
    ) -> None:
        for (
            feat,
            df,
        ) in zip(feature_sets, curves_dfs):
            logger.info(
                "{} : curve {}/{}:\n{}".format(
                    dt.datetime.now().timestamp(), dt_tm, feat, df
                )
            )

    @classmethod
    def yields_action(cls, dt_tm: dt.datetime, yields_df: pl.DataFrame) -> None:
        # is a member function so that different instances may have different methods
        logger.info(
            "{} : yields interval: {}\n{}".format(
                dt.datetime.now().timestamp(), dt_tm, yields_df
            )
        )

    @classmethod
    def process_curves(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        """Decodes a curve message and performs action on it. Current action is to print."""
        # logger.debug("client process_curves called")
        if context.session_state != SessionStateEnum.Established:
            raise ValueError("Got curve before in Established state.")
        remaining_bytes = data
        head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
        head_pad = align_by(head_len, 4)
        remaining_bytes = remaining_bytes[head_pad:]
        (dttm, feature_sets, dfs, remaining_bytes) = (
            MuniCurvesMessageFactory.deserialize_to_dataframes_no_header(
                remaining_bytes
            )
        )
        # logger.debug(f"process_curves next seq then: {context.recv_next_seq}")
        context.recv_next_seq_increment()
        # logger.debug(f"process_curves next seq now: {context.recv_next_seq}")
        cls.curves_action(dttm, feature_sets, dfs)

        return (True, remaining_bytes)

    @classmethod
    def process_yields(
        cls,
        data: bytes,
        context: SessionContext,
    ) -> Tuple[bool, bytes]:
        """Decodes a yields message and performs action on it. Current action is to print."""
        """
        logger.error("start process_yields: {}".format(dt.datetime.now().timestamp()))
        """
        # logger.debug("client process_yields called")
        if context.session_state != SessionStateEnum.Established:
            raise ValueError("Got yield before in Established state.")
        remaining_bytes = data
        head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
        head_pad = align_by(head_len, 4)
        remaining_bytes = remaining_bytes[head_pad:]
        (dt_tm, yield_df, remaining_bytes) = (
            MuniYieldPredictionMessageFactory.deserialize_to_dataframe_no_header(
                remaining_bytes
            )
        )
        # logger.debug(f"process_yields next recv seq then: {context.recv_next_seq}")
        try:
            # recv_now = context.recv_next_seq_increment()
            context.recv_next_seq_increment()
        except Exception as ex:
            logger.error(f"caught exception trying to increment recv next. {ex}")
        # logger.debug(f"process_yields next recv seq now: {recv_now}")
        # logger.error("before yields_action: {}".format(dt.datetime.now().timestamp()))

        cls.yields_action(dt_tm, yield_df)
        # logger.error("after yields_action: {}".format(dt.datetime.now().timestamp()))

        # logger.error("end process_yields: {}".format(dt.datetime.now().timestamp()))
        return (True, remaining_bytes)
