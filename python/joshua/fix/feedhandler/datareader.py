import multiprocessing as mp
from multiprocessing.connection import Client
import logging
from typing import Any, cast, Dict
import time
import polars as pl
import os
import socket

from joshua.fix.client import FixClientSession
from joshua.fix.fixp.messages import (
    Jwt,
)
from joshua.fix.client import HandleNegotiation
from joshua.fix.reader import SessionStateEnum
from joshua.fix.service.spline_data import SignalHandler
from joshua.fix.fixp.messages import (
    ClientJwt,
)


mp_ctx = mp.get_context("fork")
logger = mp_ctx.get_logger()
# logger.setLevel(logging.getLogger().level)
logger = mp_ctx.log_to_stderr()
logger.setLevel(logging.INFO)
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

signal_handler = SignalHandler()


def initialize() -> None:
    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


def client_main(cmd_ctx: Dict[str, Any]) -> int:
    """
    * config
      * address port
      * jwt from env(or create here, will need shared key in env to
        have it work with the server
      * will use new UUID each time

    * connect retry loop to server
      * on connect set TCP_NODELAY
      * pass socket to FixClientSession (this will spawn a thread/process and return)
      * send a Negotiate message
      * may want to register new handlers, defined here, to receive
        messages for predictions and for curves. Can keep in memory
        and save to disk on each iteration update. May want to load
        that interval from disk (lazyframe) on each message to ensure
        things are up to date. Or better yet, make a dataset that can
        just be added to and will maintain proper ordering.
    * have large sleep in loop(1sec) to allow for interrupts.
    * monitor can_run and send_terminate if set
    * if in terminated state, and can_run, destroy the FixClient(or in future, reset state and re-negotiate)
      return to reconnect loop

    """
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.log_to_stderr()
    logger.setLevel(cmd_ctx["loglevel"])

    if logger is None:
        logger = cast(logging.Logger, logging.getLogger())
    for name, logg in logging.root.manager.loggerDict.items():
        if name.startswith("spline"):
            logger = cast(logging.Logger, logg)
            break
    logger = cast(logging.Logger, logging.getLogger(f"{logger.name}.fix_client"))
    logger.propagate = True
    logger.setLevel(level=logging.getLogger().level)
    # logger.setLevel(logging.DEBUG)

    initialize()
    signal_handler = SignalHandler()
    ipv4addr_pair = (str(cmd_ctx["ipv4addr"]), cast(int, cmd_ctx["port"]))
    logger.debug(f"ipv4addr_pair: {ipv4addr_pair}")
    tok_gen = cmd_ctx.get("token")
    if tok_gen is None:
        raise ValueError("Cannot connect client without a token")
    logger.debug(f"token passed to client: {tok_gen}")

    while signal_handler.can_run:
        logger.debug(f"conecting to server: {ipv4addr_pair}")
        print(f"conecting to server: {ipv4addr_pair}")
        with Client(address=ipv4addr_pair, family=socket.AF_INET.name) as conn:
            # make sure that our socket has the options we want
            logger.debug("got conn from server, now get socket from it")
            conn_sock = socket.socket(fileno=conn.fileno())
            logger.debug("socket: type({}) val({})".format(type(conn_sock), conn_sock))
            # send immediately, don't wait to aggregate
            # zero=False non-zero=True
            logger.debug("set TCP_NODELAY on socket")
            conn_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            heartbeat_ms = cast(int, cmd_ctx.get("heartbeat_interval"))

            logger.debug("create FixClientSession")
            client = FixClientSession(conn=conn, heartbeat_ms=heartbeat_ms)
            # may want this to override the handlers for curves and predictions
            client_reader = client._readers[0]
            session_context = client_reader.session_context
            logger.debug("send_negotiate to server")
            HandleNegotiation.send_negotiate(
                credentials=tok_gen,
                context=session_context,
            )
            while (
                not session_context.connection.closed
                and session_context.session_state != SessionStateEnum.Terminated
                and signal_handler.can_run
            ):
                # just doing our thing, keep looping
                # logger.debug("client loop")
                time.sleep(0.1)

            logger.debug("exited client loop clean up")
            logger.debug(f"connection_closed={session_context.connection.closed}")
            logger.debug(f"session_state={session_context.session_state}")
            logger.debug(f"can_run={signal_handler.can_run}")

            if session_context.session_state != SessionStateEnum.Terminated:
                session_context.session_state = SessionStateEnum.Terminate
                client_reader.send_terminate(
                    data=bytes(),
                    context=session_context,
                    termination_str="client session cleanup",
                )
                time.sleep(0.1)
            # see if there is anything we need to clean up.
            if not session_context.connection.closed:
                session_context.connection.close()
    return 0


def main() -> int:
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.log_to_stderr()
    # logger = mp_ctx.get_logger()
    # logger.setLevel(logging.getLogger().level)
    loglevel = logging.DEBUG
    logger.setLevel(loglevel)
    logger.propagate = True
    """
    * config
      * address port
      * jwt from env(or create here, will need shared key in env to
        have it work with the server
      * will use new UUID each time

    * connect retry loop to server
      * on connect set TCP_NODELAY
      * pass socket to FixClientSession (this will spawn a thread/process and return)
      * send a Negotiate message
      * may want to register new handlers, defined here, to receive
        messages for predictions and for curves. Can keep in memory
        and save to disk on each iteration update. May want to load
        that interval from disk (lazyframe) on each message to ensure
        things are up to date. Or better yet, make a dataset that can
        just be added to and will maintain proper ordering.
    * have large sleep in loop(1sec) to allow for interrupts.
    * monitor can_run and send_terminate if set
    * if in terminated state, and can_run, destroy the FixClient(or in future, reset state and re-negotiate)
      return to reconnect loop

    """
    # Config
    cmd_ctx = {
        "ipv4addr": "3.137.181.235",
        "port": 16923,
        "work_dir": "~/client_s3",
        "heartbeat_interval": 1000,
        "loglevel": loglevel,
    }

    logger.debug("read TOKEN from env")
    token = os.getenv(key="TOKEN")
    if token is None:
        raise ValueError("You must have a TOKEN in your environment")
    token_enc = token.encode("latin1")
    tok_gen: ClientJwt = ClientJwt(token=token_enc)
    cmd_ctx["token"] = tok_gen
    return client_main(cmd_ctx)


if __name__ == "__main__":
    import sys
    import logging

    logger.setLevel(logging.DEBUG)

    sys.exit(main())
