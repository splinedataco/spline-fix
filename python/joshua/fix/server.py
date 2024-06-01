from typing import cast, Dict, List, Optional, Tuple, Union
import multiprocessing as mp
from threading import Thread
import polars as pl
import datetime as dt
from io import StringIO, BytesIO
import time
from pathlib import Path

from multiprocessing.connection import Connection

from joshua.fix.reader import SplineFixReader, SessionStateEnum, SessionContext
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory
from joshua.fix.fixp.messages import Jwt, Negotiate
from jwt import ExpiredSignatureError, InvalidIssuerError
import logging

from joshua.fix.fixp.messages import (
    NegotiationReject,
    NegotiationResponse,
    NegotiationRejectCodeEnum,
    REJECTION_REASON_STRINGS as NEGOTIATE_REJECTION_STRINGS,
    Uuid,
)
from joshua.fix.fixp.establish import (
    Establish,
    EstablishmentReject,
    EstablishmentAck,
    EstablishmentRejectCodeEnum,
    REJECTION_REASON_STRINGS as ESTABLISH_REJECTION_STRINGS,
)
from joshua.fix.fixp.terminate import (
    Terminate,
    TerminationCodeEnum,
    TERMINATE_REASON_STRINGS,
)
from joshua.fix.types import SplineNanoTime
from joshua.fix.generate import generate_message_from_type
from joshua.fix.sofh import Sofh
from joshua.fix.common import align_by, get_timestamp
from joshua.fix.messages import (
    SplineMessageHeader,
)
from joshua.fix.utils.ring_buffer import ReadonlyRingBuffer
from joshua.fix.stat_ring_buffer import StatRingBuffer

# mp_ctx = mp.get_context("spawn")
# mp_ctx = mp.get_context("fork")
start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
logger = mp_ctx.get_logger()
logger.propagate = False
# logger.setLevel(logging.root.level)
logger.setLevel(logging.WARNING)


class HandleNegotiation:
    @staticmethod
    def send_reject(
        data: bytes,
        context: SessionContext,
        reject_code: Optional[
            NegotiationRejectCodeEnum
        ] = NegotiationRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
        bad_negotiate: Optional[Negotiate] = None,
    ) -> Tuple[bool, bytes]:
        # when we need to send a reject, we either need the
        # unread bytes to get the parts out of it, or we need the
        # invalid negotiate message from which we can get the message.
        # if a reject_code is provided, other than Unspecified, it is used
        # and reject_str is ignored.
        # if a reject_code of Unspecified and a reject_str is provided, they are
        # both used.
        # if bad_negotiate is None, Unspecified is provided, and reject_str is None,
        # will try to determine the proper reject_code
        # returns if it was handled and any leftover bytes.
        session_id: Uuid
        request_timestamp: SplineNanoTime
        rej_code: NegotiationRejectCodeEnum
        rej_str: Optional[str]
        neg_rej: NegotiationReject
        neg_rej_bytes: bytes
        remaining_bytes = data
        if reject_code is not None:
            rej_code = reject_code
        else:
            rej_code = NegotiationRejectCodeEnum.Unspecified
        if bad_negotiate is not None:
            # can use this to figure out reason if needed in future
            if rej_code == NegotiationRejectCodeEnum.Unspecified and reject_str is None:
                # then try to figure out something
                jwt = bad_negotiate.credentials
                if not (jwt.curves_permitted and jwt.predictions_permitted):
                    rej_code = NegotiationRejectCodeEnum.Credentials
                    rej_str = "Credentials supplied insufficient to grant access."
                else:
                    rej_code = NegotiationRejectCodeEnum.Credentials
                    rej_str = NEGOTIATE_REJECTION_STRINGS[rej_code.value]
            else:
                # rej code is set or reject_str is set
                if reject_str is None:
                    rej_str = NEGOTIATE_REJECTION_STRINGS[rej_code.value]
                else:
                    rej_str = reject_str

            neg_rej = cast(
                NegotiationReject,
                NegotiationReject.from_negotiate(
                    bad_negotiate, reject_code=rej_code, reject_str=rej_str
                ),
            )

        else:
            # something failed in negotiation, like the jwt is invalid
            # since we didn't actually create a negotiate message, we'll
            # get what parts we can
            remaining_bytes = data
            (parts, remaining_bytes) = Negotiate.deserialize_to_dict(remaining_bytes)
            si = parts.get("sessionId")
            session_id = si if si is not None else Uuid()
            rt: Optional[SplineNanoTime] = parts.get("timestamp")
            request_timestamp = rt if rt is not None else SplineNanoTime()
            # need the reject code and string.
            if (
                rej_code == NegotiationRejectCodeEnum.Unspecified
                and reject_str is not None
            ):
                rej_str = reject_str
            else:
                rej_str = NEGOTIATE_REJECTION_STRINGS[rej_code.value]

            neg_rej = NegotiationReject(
                session_id=session_id,
                request_timestamp=request_timestamp,
                reject_code=rej_code,
                reject_str=rej_str,
            )
        neg_rej_bytes = generate_message_from_type(
            msg=neg_rej, num_groups=neg_rej.num_groups
        )
        logger.debug("negotiation attempt failed returning reject: {}".format(neg_rej))
        context.connection_lock.acquire()
        try:
            context.connection.send_bytes(neg_rej_bytes)
        finally:
            context.connection_lock.release()

        # session level bump time but not seq
        context.set_send_time_last_message(None)
        logger.debug(
            f"send_negotiation_reject: sent {len(neg_rej_bytes)} bytes to client"
        )
        context.session_state = SessionStateEnum.Terminating
        HandleTerminate.terminate_session(context=context, termination_str=reject_str)
        # TODO: call send_terminate here.
        return (True, remaining_bytes)

    @classmethod
    def process_negotiate(
        cls, data: bytes, context: SessionContext
    ) -> Tuple[bool, bytes]:
        """Take byte buffer
        Assumes caller has already properly determined this message handler
        and has consumed the sofh, but not any padding.

        returns (handled: bool, remaining_bytes: bytes)
        """
        if not isinstance(context, SessionContext):
            raise NotImplementedError
        # header should already have been consumed
        remaining_bytes = data
        # but we need to account for any padding there may be
        head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
        head_pad = align_by(head_len, 4)
        remaining_bytes = remaining_bytes[head_pad:]

        try:
            (negotiate_msg, remaining_bytes) = Negotiate.deserialize(remaining_bytes)
            negotiate_msg = cast(Negotiate, negotiate_msg)

        except (ExpiredSignatureError, InvalidIssuerError, ValueError) as ex:
            logger.debug("Negotiation Failed. {}".format(ex))
            reject_code: NegotiationRejectCodeEnum
            reject_str: Optional[str] = None
            if isinstance(ex, ExpiredSignatureError):
                reject_code = NegotiationRejectCodeEnum.Credentials
            elif isinstance(ex, InvalidIssuerError):
                reject_code = NegotiationRejectCodeEnum.Credentials
            else:
                reject_code = NegotiationRejectCodeEnum.Unspecified
                reject_str = str(ex)

            logger.debug("negotiation attempt failed returning reject")

            (handled, remaining_bytes) = cls.send_reject(
                data=remaining_bytes,
                context=context,
                reject_code=reject_code,
                reject_str=reject_str,
                bad_negotiate=None,
            )
            return (handled, remaining_bytes)
        # otherwise we got a negotiate message
        # and it is valid. Doesn't mean that they are authorized though
        # here we have a negotiation message, but don't know to allow it or note
        # TODO: verify that sessionid is unique for day.
        # need to have a global, preferably one that is persisted to disk/network
        # NOTE: Because we are a datafeed and will respin the most recent update
        # on connect, there is no need to track the sessionid really, every connection
        # can just use a new sessionid and start fresh. And this should be the recommended
        # method of connecting. Effectively it means that sessionid isn't used.
        logger.debug(f"proffered session id: {negotiate_msg.sessionId}")
        # since we have a jwt, they are authenticated and have a valid jwt
        # We need to see what feeds to turn on for the
        # TODO: register messages with reader for feeds/messages client is authorized to receive
        # NOTE: we want to defer the registration until after the session is set up
        # or we may send data before then.
        logger.debug("Got negotiation attempt: {}".format(negotiate_msg))
        # here we would validate the login attempt and validate feeds by acl
        jwt = negotiate_msg.credentials
        # NOTE: we're using the checks directly from the jwt not the context
        # because we've received the jwt in this thread/proc so we have the most up
        # to date values.
        if jwt.curves_permitted and jwt.predictions_permitted:
            neg_resp = NegotiationResponse(
                session_id=negotiate_msg.sessionId,
                request_timestamp=negotiate_msg.timestamp,
            )
            neg_resp_bytes = generate_message_from_type(
                msg=neg_resp,
                num_groups=neg_resp.num_groups,
            )
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(neg_resp_bytes)
            finally:
                context.connection_lock.release()

            # session level bump time but not seq
            context.set_send_time_last_message(None)
            logger.debug(f"NegotiationResponse: sc {len(neg_resp_bytes)} bytes")
            # TODO: if/when we have the ability to globally track uuids
            # before adding here, and returning negotiated, we'd want to verify
            # that the uuid being taken over belonged to the user that was logger.in
            # so we should store the jwt along with the session id and will likely
            # want a multimap allowing us to look up session id directly, or by
            # userid.
            context.session_state = SessionStateEnum.Negotiated
            context.session_id = negotiate_msg.sessionId
            context.credentials = negotiate_msg.credentials
            return (True, remaining_bytes)
        else:
            (handled, remaining_bytes) = cls.send_reject(
                data=remaining_bytes,
                context=context,
                bad_negotiate=negotiate_msg,
            )
            return (handled, remaining_bytes)


class HandleEstablishment:
    @staticmethod
    def send_reject(
        data: bytes,
        context: SessionContext,
        reject_code: Optional[
            EstablishmentRejectCodeEnum
        ] = EstablishmentRejectCodeEnum.Unspecified,
        reject_str: Optional[str] = None,
        bad_establishment: Optional[Establish] = None,
    ) -> Tuple[bool, bytes]:
        """This can be reached in a few states, WaitingForConnect, Negotiate, Negotiated,
        and Establish, and Established.
        So the bad_establishment won't always be available to retrieve the info from.
        """
        # populate the fields we will need
        session_id: Uuid
        if context.session_id is not None:
            session_id = context.session_id
        elif bad_establishment is not None:
            # then we don't have a uuid for this session, but we got an establish.
            # this could happen if an establish is sent before negotiation
            session_id = bad_establishment.sessionId
        else:
            # we'll just make up one.
            session_id = Uuid()

        request_timestamp: SplineNanoTime
        if bad_establishment is not None:
            request_timestamp = bad_establishment.timestamp_nano
        else:
            request_timestamp = SplineNanoTime(get_timestamp())

        rej_code: EstablishmentRejectCodeEnum
        rej_str: Optional[str]
        if reject_code is not None:
            rej_code = reject_code
        else:
            rej_code = EstablishmentRejectCodeEnum.Unspecified

        rej_str = ESTABLISH_REJECTION_STRINGS[rej_code.value]
        if reject_str is not None:
            rej_str += reject_str

        est_rej: EstablishmentReject
        est_rej_bytes: bytes
        remaining_bytes = data

        est_rej = cast(
            EstablishmentReject,
            EstablishmentReject(
                session_id=session_id,
                request_timestamp=request_timestamp,
                reject_code=rej_code,
                reject_str=rej_str,
            ),
        )
        est_rej_bytes = generate_message_from_type(
            msg=est_rej, num_groups=est_rej.num_groups
        )
        logger.warning("Failed Establish attempt: {}".format(est_rej))
        context.connection_lock.acquire()
        try:
            context.connection.send_bytes(est_rej_bytes)
        finally:
            context.connection_lock.release()

        # session level bump time but not seq
        context.set_send_time_last_message(None)
        # don't think we need to change the current session state on reject
        if bad_establishment is not None:
            return (True, remaining_bytes)
        else:
            # consume the size of the message
            pos = Sofh.find_sofh(data=remaining_bytes)
            if pos is None:
                pos = len(remaining_bytes)
            return (True, remaining_bytes[pos:])

    @classmethod
    def process_establish(
        cls, data: bytes, context: SessionContext
    ) -> Tuple[bool, bytes]:
        """Take byte buffer
        Assumes caller has already properly determined this message handler
        and has consumed the sofh, but not any padding.

        returns (handled: bool, remaining_bytes: bytes)
        """
        # TODO:
        # [] check state of connection, if not negotiated, then reject

        logger.debug(
            f"client({context.is_client}) processing Establish message. check state={context.session_state} jwt:={context.credentials}"
        )
        server_state = context.session_state
        logger.debug(
            "client({}): process_establish: server_state: {}".format(
                context.is_client, server_state
            )
        )
        if server_state != SessionStateEnum.Negotiated:
            logger.error(
                f"client({context.is_client}) process_enrich called but not negotiated. Rejecting."
            )
            rej_code: EstablishmentRejectCodeEnum
            if server_state == SessionStateEnum.WaitingOnConnect:
                rej_code = EstablishmentRejectCodeEnum.Unnegotiated
            elif server_state == SessionStateEnum.Established:
                rej_code = EstablishmentRejectCodeEnum.AlreadyEstablished
            else:
                rej_code = EstablishmentRejectCodeEnum.Unspecified
            return cls.send_reject(
                data=data,
                context=context,
                reject_code=rej_code,
                reject_str=None,
                bad_establishment=None,
            )
        # means we're in the correct Negotiate state
        # header should already have been consumed
        remaining_bytes = data
        # but we need to account for any padding there may be
        head_len = Sofh.struct_size_bytes() + SplineMessageHeader.struct_size_bytes()
        head_pad = align_by(head_len, 4)
        remaining_bytes = remaining_bytes[head_pad:]

        try:
            logger.debug(f"client({context.is_client}) deserializing Establish")
            (establish_msg, remaining_bytes) = Establish.deserialize(remaining_bytes)
            establish_msg = cast(Establish, establish_msg)
            logger.debug(
                f"client({context.is_client}) Establish.deserialized:{establish_msg}"
            )

        except ValueError as ex:
            logger.debug("Failed to Establish. {}".format(ex))
            reject_code = EstablishmentRejectCodeEnum.Unspecified
            reject_str = f"{ex}"

            logger.error("establish attempt failed; returning reject")

            (handled, remaining_bytes) = cls.send_reject(
                data=remaining_bytes,
                context=context,
                reject_code=reject_code,
                reject_str=reject_str,
                bad_establishment=None,
            )
            return (handled, remaining_bytes)
        # otherwise we got a establish message
        # and it is valid. They've already authorized, so all we're doing
        # is agreeing on sequence number and keep alive interval
        logger.debug(
            f"proffered session id: {establish_msg.sessionId} negotiated session id: {context.session_id}"
        )
        if establish_msg.sessionId != context.session_id:
            logger.debug(
                "Failed to Establish; SessionId {} doesn't match. {}".format(
                    establish_msg.sessionId, context.session_id
                )
            )
            reject_code = EstablishmentRejectCodeEnum.Unspecified
            reject_str = f"session_id: {establish_msg.sessionId} does not match negotiated: {context.session_id}"

            logger.debug("establish attempt failed; returning reject")

            (handled, remaining_bytes) = cls.send_reject(
                data=remaining_bytes,
                context=context,
                reject_code=reject_code,
                reject_str=reject_str,
                bad_establishment=None,
            )
            return (handled, remaining_bytes)
        # since we have a jwt, they are authenticated and have a valid jwt
        # We need to see what feeds to turn on for the
        # TODO: register messages with reader for feeds/messages client is authorized to receive
        # NOTE: we want to defer the registration until after the session is set up
        # or we may send data before then.
        # since we're allowing establishment, accept their keep alive interval
        context.recv_hb_interval_ms = establish_msg.keep_alive
        logger.debug(f"client({context.is_client}) Create and send EstablishmentAck")
        est_ack = EstablishmentAck(
            session_id=establish_msg.sessionId,
            request_timestamp=establish_msg.timestamp,
            keep_alive_ms=context.send_hb_interval_ms,
            server_sequence_num=context.send_next_seq,
        )
        est_ack_bytes = generate_message_from_type(
            msg=est_ack,
            num_groups=est_ack.num_groups,
        )
        logger.debug(
            f"client({context.is_client}) send EstablishmentAck to client({not context.is_client})"
        )
        try:
            logger.debug(
                f"""client({context.is_client}) send EstablishmentAck to client({not context.is_client}).\n{est_ack_bytes.hex(":")}"""
            )
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(est_ack_bytes)
            finally:
                context.connection_lock.release()

        except Exception as ex:
            logger.error(
                f"client({context.is_client}) Exception sending EstablishmentAck to {not context.is_client}.\n{ex}"
            )
        logger.debug(
            f"client({context.is_client}) EstablishmentAck sent to client({not context.is_client})"
        )

        # we sent a session level message, which doesn't get a heartbeat
        # but we do want to update our last sent timer
        context.set_send_time_last_message(None)
        logger.debug(f"EstablishmentAck: sc {len(est_ack_bytes)} bytes")
        # TODO: when successful, compare the suggested client_next_seq
        # vs the one in the context. If they don't match then we need to
        # figure out what to do. If cs: client_next_seq > sc: client_next_seq
        # it implies missed messages. We'd send a gap request normally, but we
        # really should just have them connect with a new session id and start
        # from 1.
        # if cs: client_next_seq < sc: client_next_seq is bad/not possible, and
        # should cause a terminate message to be sent and the client should reconnect
        # with a new session id.

        context.session_state = SessionStateEnum.Established
        logger.debug(f"end of send EstablishmentAck. context={context}")
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

        # populate the fields we will need
        session_id: Uuid
        if context.session_id is not None:
            session_id = context.session_id
        else:
            # we don't have even a session_id. Just close the connection.
            logger.debug("HandleTerminate closing connection.")
            context.connection.close()
            context.session_state = SessionStateEnum.Terminated

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
        remaining_bytes = data

        term = cast(
            Terminate,
            Terminate(
                session_id=session_id,
                termination_code=term_code,
                termination_str=term_str,
            ),
        )
        term_bytes = generate_message_from_type(msg=term, num_groups=term.num_groups)
        logger.info("Terminate Session: {}".format(term))
        if not context.connection.closed and context.connection.writable:
            context.connection_lock.acquire()
            try:
                context.connection.send_bytes(term_bytes)
            except Exception as ex:
                logger.warning(
                    "Got exception trying to send terminate message. Treating connection as terminated. {}".format(
                        ex
                    )
                )
                context.set_send_time_last_message(None)
                context.session_state = SessionStateEnum.Terminated
            else:
                # update last time. session level message so no seq # increment
                context.set_send_time_last_message(None)
                context.session_state = SessionStateEnum.Terminating
            finally:
                context.connection_lock.release()

        else:
            context.set_send_time_last_message(None)
            context.session_state = SessionStateEnum.Terminated

        # consume the size of the message, but stop if there is another
        # message in the feed.
        pos = Sofh.find_sofh(data=remaining_bytes)
        if pos is None:
            pos = len(remaining_bytes)
        return (True, remaining_bytes[pos:])

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
            logger.debug("server.terminate_session closing connection")
            context.connection.close()
        context.session_state = SessionStateEnum.Terminated
        context.cancel_timer(context._recv_hb_timer)
        context.cancel_timer(context._send_hb_timer)

    @classmethod
    def process_terminate(
        cls, data: bytes, context: SessionContext
    ) -> Tuple[bool, bytes]:
        logger.debug(f"server process_terminate: session_state={context.session_state}")
        # sofh and header already consumed. And we've been called because we received a terminate
        remaining_bytes = data
        (terminate_msg, remaining_bytes) = Terminate.deserialize(remaining_bytes)
        if context.session_state == SessionStateEnum.Terminating:
            logger.debug(
                "server received termination response while in Terminating state. Switching to terminated state and closing connection"
            )
            # then we've received our response
            context.session_state = SessionStateEnum.Terminated
            if not context.connection.closed:
                context.connection.close()
        else:
            # we need to send one back
            # based on the spec, "Upon receiving a Terminate message, the receiver must
            # respond with a Terminate message. The Terminate response must be the last message sent."
            logger.debug("server send_terminate in response")
            (handled, data) = cls.send_terminate(remaining_bytes, context=context)
            # give some time to let the client receive the response
            # heartbeat should continue, so we can receive the terminate
            # response. And if we don't get it, the heartbeat expiration
            # should eventually close the connection for us
            # cls.terminate_session(context)
        return (True, data)


class FixServerSession:
    # TODO: readers probably needs to be an object that contains
    #       process, uuid, userid, and server_conn to the reader.
    #       the server then needs to add in a select() from all of
    #       the server_connections to know how/what to check, respond,
    #       and control. need to be able to recieve msg from a connection
    #       and be able to map to it's reader. Preferably uuid, perhaps
    #       have a separate uuid for each reader so that when in negotiate
    #       phase it has a uuid. After negotiate, it will have 2, 1 of which
    #       can be used to identify itself to the server.
    # about 75k messages needed to send 1 predictions file
    # messages are about 1488b so 1000x is 1_488_000b
    # 100x is 148_800b
    msg_max_batch_size = 100
    _readers_proc: List[mp.context.ForkProcess] = []
    _readers: List[SplineFixReader] = []
    _server_credentials: Optional[Jwt]
    _uuid_to_reader: Dict[str, SplineFixReader]
    # TODO:
    # email/userid to session/reader (each gets 1)
    # uuid->(email,reader)
    # when successfully negotiating, can then check for uniqueness
    # of uuid. If belongs to same id, can attempt a resume.
    # If userid has a different uuid already, terminate the other
    # session and store the new uuid. If uuid belongs to another
    # userid, reject with duplicate uuid message.
    # Can keep all established sessions in memory: if crash, all
    # sessions can just be reestablished. Max number of established
    # sessions should only ever be the number of clients.

    def get_unique_uuid(self) -> Uuid:
        return Uuid()

    def uuid_is_unique(self) -> bool:
        return True

    def __init__(
        self,
        conn: Connection,
        stop_at_eof: bool = False,
        conn_other_end: Optional[Connection] = None,
        heartbeat_ms: int = 1000,
        ring_buffer_path: Optional[Union[Path, str]] = None,
    ) -> None:
        self._ring_buffer_path = ring_buffer_path
        self._prediction_path_tail = "predictions"
        self._curve_path_tail = "curves"
        self._server_credentials = None
        self._readers = []
        self.logger = mp_ctx.get_logger()
        # logger.setLevel(logging.root.level)
        self.logger.setLevel(logging.WARNING)

        reader = SplineFixReader(is_client=False, initial_heartbeat_ms=heartbeat_ms)
        reader.register_byte_stream(conn)

        # TODO: move initialize section into a function
        #       and have a socket accept here with a select/poll
        #       that sets up a new reader and hands it the incoming socket.
        # TODO: consider 2 pipes, or socket, pipe. Second one is for
        #       bidirectional reader/Service communication to check uuid
        #       or to resume/adopt a prior session.

        # register handlers here first
        # set negotiate messages.
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
            # the server has to close it's copy of the client
            # file descriptor so as to be notified of EOF
            session_context.other_end_connection = conn_other_end
        reader.session_context = session_context
        reader = self.register_negotiate_handler(reader=reader)
        reader = self.register_establish_handler(reader=reader)
        reader = self.register_terminate_handler(reader=reader)
        reader = self.register_send_terminate(reader=reader)
        sp = mp_ctx.Process(
            target=reader.reader_start,
            name="fix_server",
            kwargs={"stop_at_eof": stop_at_eof},
            daemon=False,
        )
        sp.start()
        self._readers_proc.append(sp)
        self._readers.append(reader)

        if self._ring_buffer_path:
            self.logger.debug(
                f"self._ring_buffer_path={self._ring_buffer_path} start monitor_and_send"
            )
            monit = mp_ctx.Process(
                target=self.monitor_and_send,
                name="publish",
                daemon=False,
            )
            monit.start()
            self.logger.debug(f"started monit: {monit}")
            self._monitor = monit

    def stop_all(self) -> None:
        # TODO:
        # for every reader, terminate and then join
        logger.warning("server.stop_all() called")
        if self._ring_buffer_path:
            # stop sending data
            if self._monitor.is_alive():
                self._monitor.join(0.1)
                if self._monitor.is_alive():
                    self._monitor.kill()
                    self._monitor.join(0.1)
                self._monitor.close()
        for r in self._readers:
            context = r.session_context
            context.cancel_timer(context._recv_hb_timer)
            context.cancel_timer(context._send_hb_timer)
            try:
                if not r.session_context.connection.closed:
                    r.session_context.connection.close()
            except IOError as ex:
                logger.debug(f"server connection already closed. {ex}")
            logger.warning("stopped all")

        for rp in self._readers_proc:
            rp = cast(Thread, rp)
            logger.warning("calling rp.join() on {}".format(r))
            try:
                rp.join(0.1)
                if rp.is_alive():
                    logger.error("rp.terminate() being called")
                    rp.terminate()
            except ValueError as ex:
                if str(ex).endswith("process object is closed"):
                    pass
                else:
                    raise ex
            else:
                logger.warning("rp.join() being called")
                rp.join(0.1)
                rp.kill()
                rp.join(0.1)
                logger.warning("called rp.join() and rp.kill()")

    def register_negotiate_handler(self, reader: SplineFixReader) -> SplineFixReader:
        """When initiating a new connection this state will be active."""
        reader.register_handler(
            Negotiate._messageType.value,
            HandleNegotiation.process_negotiate,
            Negotiate._schemaId,
            Negotiate._version,
        )
        reader.session_state = SessionStateEnum.WaitingOnConnect
        return reader

    def register_establish_handler(self, reader: SplineFixReader) -> SplineFixReader:
        """After successful negotiation."""
        reader.register_handler(
            Establish._messageType.value,
            HandleEstablishment.process_establish,
            Establish._schemaId,
            Establish._version,
        )
        return reader

    def register_terminate_handler(self, reader: SplineFixReader) -> SplineFixReader:
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

    def __str__(self) -> str:
        buf = StringIO()
        buf.write("FixServerSession:\n")
        buf.write("    connection.closed: {}\n".format(self._readers[0].conn.closed))
        buf.write("    pid: {}\n".format(self._readers_proc[0].pid))
        if self._server_credentials:
            buf.write("    server_credentials: {}\n".format(self._server_credentials))
        else:
            buf.write("    default jwt: {}\n".format(Jwt()))

        buf.write("    uuid: {}\n".format(self._readers[0].session_context.session_id))
        return buf.getvalue()

    def send_curves(
        self, dt_tm: dt.datetime, feature_sets: List[str], curves: List[pl.DataFrame]
    ) -> int:
        if not self._readers[0].session_context.curves_permitted():
            logging.debug("send_curves: not_permitted")
            return -1
        return HandleApplication.send_curves(
            context=self._readers[0].session_context,
            dt_tm=dt_tm,
            feature_sets=feature_sets,
            curves=curves,
        )

    def send_yields(self, dt_tm: dt.datetime, yields: pl.DataFrame) -> int:
        if not self._readers[0].session_context.predictions_permitted():
            logging.debug("send_predictions: not_permitted")
            return -1
        return HandleApplication.send_yields(
            context=self._readers[0].session_context, dt_tm=dt_tm, yields=yields
        )

    def find_next_sofh(self, rb: ReadonlyRingBuffer) -> int:
        # NOTE/WARNING: don't read from the file as this will
        # change the write pointer which will cause conflicts
        # with other threads do to being lock-free.
        # we'll get a copy of the buffer and work with the buffer
        # if we can
        last_pos = rb.last_pos
        test_buffer = rb._file_mmap[last_pos:]
        test_buffer += rb._file_mmap[:last_pos]
        next_sofh = Sofh.find_sofh(test_buffer)
        if not next_sofh:
            return last_pos
        else:
            return int(next_sofh)

    @staticmethod
    def seek_next_sofh(rb: ReadonlyRingBuffer) -> ReadonlyRingBuffer:
        # def seek_next_sofh(rb: ReadonlyRingBuffer) -> ReadonlyRingBuffer:
        logging.basicConfig(
            level=logging.WARNING,
            format="%(asctime)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        logger = logging.getLogger()
        logger.propagate = False
        logger.setLevel(logging.WARNING)

        # print(f"seek_first_sofh: rb\n{rb}")
        last_written_pos = rb.last_pos
        # print(f"last_pos={last_written_pos}")
        curr_pos = rb._file_mmap.tell()
        # print(f"curr_pos={curr_pos}")
        if last_written_pos == curr_pos:
            # print("last_pos == curr_pos leaving alone")
            return rb
        # find the next sofh
        sofh_size = Sofh.struct_size_bytes()
        interval = 1500
        bytes_avail = rb.bytes_available_to_read()
        if bytes_avail < sofh_size:
            rb._file_mmap.seek(last_written_pos, 0)
            print("not many bytes available, seeking last_pos")
            return rb
        potential_sofh_bytes = rb.read(sofh_size + interval)
        sofh_pos = Sofh.find_sofh(potential_sofh_bytes)
        bytes_avail -= len(potential_sofh_bytes)
        while sofh_pos is None and bytes_avail > 0:
            curr_pos = rb._file_mmap.tell()
            next_amt = max(sofh_size + interval, bytes_avail)
            # we want to back up one sofh_size in case we're mid
            # header (probably only need sofh_size-1) and
            # then we'll add the next chunk
            potential_sofh_bytes = potential_sofh_bytes[
                len(potential_sofh_bytes) - sofh_size :
            ]
            next_bytes = rb.read(next_amt)
            bytes_avail -= len(next_bytes)
            potential_sofh_bytes += next_bytes
            sofh_pos = Sofh.find_sofh(potential_sofh_bytes)

        if sofh_pos is not None:
            # print(f"found next_sofh at {curr_pos}+{sofh_pos}, seeking to next_sofh")
            rb._file_mmap.seek(curr_pos + sofh_pos, 0)
        else:
            # we didn't find anything, start at last_pos
            print(f"did not find next_sofh, seeking to last_pos={rb.last_pos}")
            rb._file_mmap.seek(rb.last_pos, 0)
        return rb

    def monitor_and_send(self) -> None:
        self.logger.debug(
            f"monitor_and_send. if self._ring_buffer_path is None return. _ring_buffer_path={self._ring_buffer_path}"
        )
        if self._ring_buffer_path is None:
            return
        self._ring_buffer_path = cast(Path, Path(self._ring_buffer_path))
        curves_path = self._ring_buffer_path / self._curve_path_tail
        self.logger.debug(f"curves_path = {curves_path}")
        predictions_path = self._ring_buffer_path / self._prediction_path_tail
        self.logger.debug(f"predictions_path = {predictions_path}")

        # we may have a path but it may not exist yet.
        self.logger.debug(
            f"check for curves.exists(): {curves_path.exists()} and predictions.exist(): {predictions_path.exists()} in loop"
        )
        while not curves_path.exists() or not predictions_path.exists():
            if not curves_path.exists():
                self.logger.debug(f"waiting for curves_path to exist: {curves_path}")
            if not predictions_path.exists():
                self.logger.debug(
                    f"waiting for predictions_path to exist: {predictions_path}"
                )
            time.sleep(0.5)
        # means that it should exist by now
        self.logger.debug(f"server open ReadonlyRingBuffer({curves_path})")
        self._curves_ring_buffer = ReadonlyRingBuffer(filename=curves_path)
        cb = self._curves_ring_buffer
        cb_has_wrapped = StatRingBuffer.has_wrapped(cb)
        if cb_has_wrapped:
            # then find the next sofh after the last_write (the point
            # just past where we started overwriting prior data)
            last_pos = cb.last_pos + 1
            cb._file_mmap.seek(last_pos, 0)
        else:
            cb._file_mmap.seek(0, 0)
        StatRingBuffer.seek_next_sofh(cb)
        # next_curve_sofh = self.find_next_sofh(rb=self._curves_ring_buffer)
        # self._curves_ring_buffer._file_mmap.seek(next_curve_sofh, 0)
        self.logger.debug(f"after server open ReadonlyRingBuffer({curves_path})")
        self.logger.debug(f"server open ReadonlyRingBuffer({predictions_path})")
        self._predictions_ring_buffer = ReadonlyRingBuffer(filename=predictions_path)
        pb = self._predictions_ring_buffer
        pb_has_wrapped = StatRingBuffer.has_wrapped(pb)
        if pb_has_wrapped:
            last_pos = pb.last_pos + 1
            pb._file_mmap.seek(last_pos, 0)
        else:
            pb._file_mmap.seek(0, 0)
        StatRingBuffer.seek_next_sofh(pb)
        # next_prediction_sofh = self.find_next_sofh(rb=self._predictions_ring_buffer)
        # self._predictions_ring_buffer._file_mmap.seek(next_prediction_sofh)
        self.logger.debug(f"after server open ReadonlyRingBuffer({predictions_path})")
        # easiest at first is to set to last_pos and wait for the next batch of data to send.
        # TODO: start at pos=0 and find_sofh() then rb.seek(sofhpos, 0) and
        # start reading processing.
        self.logger.debug("enter loop")
        while True:
            sstate = self._readers[0].session_state
            # self.logger.debug(f"monit loop: sstate={sstate}")
            if (
                sstate == SessionStateEnum.Terminate
                or sstate == SessionStateEnum.Terminating
                or sstate == SessionStateEnum.Terminated
            ):
                break
            if sstate != SessionStateEnum.Established:
                time.sleep(0.1)
                self.logger.debug("not established yet")
                continue
            # else, lets look for work to do.
            # self.logger.debug("server look for work")
            context = self._readers[0].session_context
            if context.curves_permitted():
                # logger.debug("monit curves permitted")
                # self.logger.debug("curves_permitted, check for bytes to read")
                # logger.debug(
                #     f"curves.bytes_avail() {self._curves_ring_buffer.bytes_available_to_read()}"
                # )
                seq_start = context.send_next_seq
                sofh_size = Sofh.NUM_BYTES
                avail_bytes = self._curves_ring_buffer.bytes_available_to_read()
                while avail_bytes > 0:
                    send_buf = BytesIO()
                    sofh_bytes = self._curves_ring_buffer.read(size=sofh_size)
                    send_buf.write(sofh_bytes)
                    (sofh, _) = Sofh.deserialize(sofh_bytes)
                    sofh = cast(Sofh, sofh)
                    message_body_len = sofh.message_body_len()
                    message_body = self._curves_ring_buffer.read(size=message_body_len)
                    send_buf.write(message_body)
                    # this is enough to send one message. We know it's a curve because it's in this file
                    # But don't need to know.

                    context.connection_lock.acquire()
                    try:
                        context.connection.send_bytes(buf=send_buf.getvalue())
                    finally:
                        context.connection_lock.release()

                    next_seq = context.send_next_seq_increment()
                    if (next_seq - seq_start) > self.msg_max_batch_size:
                        break
                    avail_bytes -= sofh_size + message_body_len
            if context.predictions_permitted():
                seq_start = context.send_next_seq
                # logger.debug("monit predictions permitted")
                # self.logger.debug("predictions_permitted, check for bytes to read")
                # logger.debug(
                #     f"predictions.bytes_avail() {self._predictions_ring_buffer.bytes_available_to_read()}"
                # )

                avail_bytes = self._predictions_ring_buffer.bytes_available_to_read()
                sofh_size = Sofh.NUM_BYTES
                # NOTE: we're reading from a file, not a socket. Once the data is written the position
                # is advanced, so will always be complete messages
                while avail_bytes > 0:
                    # get just the sofh
                    send_buf = BytesIO()
                    sofh_bytes = self._predictions_ring_buffer.read(size=sofh_size)
                    send_buf.write(sofh_bytes)
                    (sofh, _) = Sofh.deserialize(sofh_bytes)
                    sofh = cast(Sofh, sofh)
                    message_body_len = sofh.message_body_len()
                    message_body = self._predictions_ring_buffer.read(
                        size=message_body_len
                    )
                    send_buf.write(message_body)
                    # this is enough to send one message. We know it's a prediction because it's in this file
                    # but don't need to know.

                    context.connection_lock.acquire()
                    try:
                        context.connection.send_bytes(buf=send_buf.getvalue())
                    finally:
                        context.connection_lock.release()

                    next_seq = context.send_next_seq_increment()
                    if (next_seq - seq_start) > self.msg_max_batch_size:
                        break
                    avail_bytes -= sofh_size + message_body_len


class HandleApplication:
    @staticmethod
    def send_curves(
        context: SessionContext,
        dt_tm: dt.datetime,
        feature_sets: List[str],
        curves: List[pl.DataFrame],
    ) -> int:
        """Given interval as datetime, a list of feature sets, and a parallel list of curves as dataframes
        serializes them and sends them to client.
        returns number of messages sent
        """
        logger.debug(
            "HandleApplication.send_curves: curves_permitted\n{}".format(
                context.curves_permitted()
            )
        )
        if context.session_state != SessionStateEnum.Established:
            raise ValueError(
                "session must be established before sending application data"
            )
        if not context.curves_permitted():
            raise PermissionError("session not permitted for curves")

        # based on the number of curves, we may not be able to fit them
        # all in a single message
        num_messages = 0
        for curve_bytes in MuniCurvesMessageFactory.serialize_from_dataframes(
            dt_tm=dt_tm, feature_sets=feature_sets, dfs=curves
        ):
            if not context.connection.closed and context.connection.writable:
                context.connection_lock.acquire()
                try:
                    context.connection.send_bytes(curve_bytes)
                finally:
                    context.connection_lock.release()

                curr_send_seq = context.send_next_seq_increment()
                logger.debug(f"send_curves: curves sent. send seq now: {curr_send_seq}")
                num_messages += 1
            else:
                raise EOFError(
                    "send_curves found context.connection closed or unwratable"
                )
        logger.debug(f"send_curves sent {num_messages} total curve messages.")
        return num_messages

    @staticmethod
    def send_yields(
        context: SessionContext, dt_tm: dt.datetime, yields: pl.DataFrame
    ) -> int:
        """Takes the interval and a single dataframe with the yields for that interval, serializes them, and sends
        them to the client, using the context.
        returns number of messages sent.
        """
        if context.session_state != SessionStateEnum.Established:
            raise ValueError(
                "session must be established before sending application data"
            )
        if not context.predictions_permitted():
            raise PermissionError("session not permitted for yield predictions")

        num_messages = 0
        for yield_bytes in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
            dt_tm, yields
        ):
            if not context.connection.closed and context.connection.writable:
                context.connection_lock.acquire()
                try:
                    context.connection.send_bytes(yield_bytes)
                finally:
                    context.connection_lock.release()

                curr_send_seq = context.send_next_seq_increment()
                logger.debug(f"send_yields: yields sent. send seq now: {curr_send_seq}")
                num_messages += 1
            else:
                raise EOFError(
                    "send_curves found context.connection closed or unwritable"
                )
        logger.debug(f"send_yields sent {num_messages} total yield messages.")
        return num_messages
