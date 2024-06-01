import pytest
from typing import cast
import multiprocessing as mp
from multiprocessing import Process
from multiprocessing.connection import Client
import logging
import socket
import time
import datetime as dt
import polars as pl
from tempfile import TemporaryDirectory

# used to kill a process when normal methods don't work
import os
import subprocess

from joshua.fix.client import FixClientSession
from joshua.fix.fixp.messages import (
    Jwt,
)
from joshua.fix.client import HandleNegotiation
from joshua.fix.reader import SessionStateEnum
from joshua.fix.service import spline_data

start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")

mp_ctx = mp.get_context(mp.get_start_method())
# test_logger = mp_ctx.get_logger()
# logger.setLevel(logging.DEBUG)
# test_logger = logging.getLogger()
# test_logger = mp_ctx.get_logger()
test_logger = mp_ctx.log_to_stderr(logging.DEBUG)
test_logger.setLevel(logging.getLogger().level)
# signal_handler = spline_data.SignalHandler()
# spline_data.signal_handler = signal_handler


def initialize() -> None:
    # logger.getLogger().setLevel(logger.DEBUG)
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_width_chars(300)
    pl.Config.set_tbl_cols(40)


# @pytest.mark.timeout(timeout=8)
@pytest.mark.skip("leaves processes")
@pytest.mark.timeout(timeout=8, method="signal")
def test_network_service_with_client() -> None:
    """To run this and get debug logger output
    pytest --capture=tee-sys --log-level=DEBUG -k test_network_service_with_client -o log_cli=true
    """
    initialize()
    test_logger = mp_ctx.log_to_stderr(logging.DEBUG)
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

    made_up_unique_dir = TemporaryDirectory()
    cmd_ctx = {
        "ipv4addr": "127.0.0.1",
        "port": 36000,
        "work_dir": "~/s3",
        "heartbeat_interval": 500,
        "ring_buffer_prefix": made_up_unique_dir.name,
    }
    signal_handler = spline_data.SignalHandler()
    test_logger.debug("call process with running_start")
    rs = Process(
        target=spline_data.running_start, args=((cmd_ctx), signal_handler), daemon=False
    )
    test_logger.debug("start running_start")
    rs.start()

    # signal_handler = spline_data.SignalHandler()
    # test_logger.debug("sleep(1.0)")
    time.sleep(0.1)
    # now we need a client
    port: int = int(str(cmd_ctx["port"]))
    ipv4addr_pair = (str(cmd_ctx["ipv4addr"]), port)
    with Client(address=ipv4addr_pair, family=socket.AF_INET.name) as conn:
        # make sure that our socket has the options we want
        conn_sock = socket.socket(fileno=conn.fileno())
        # send immediately, don't wait to aggregate
        # zero=False non-zero=True
        conn_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        heartbeat_ms: int = cast(int, cmd_ctx.get("heartbeat_interval"))

        client = FixClientSession(conn=conn, heartbeat_ms=heartbeat_ms)
        start = dt.datetime.now()
        max_sec = 3
        cc = client._readers[0].session_context

        # need this to "prime the pump"
        test_logger.debug("send_negotiate from client")
        HandleNegotiation.send_negotiate(
            credentials=tok_gen,
            context=cc,
        )

        while cc.session_state != SessionStateEnum.Established:
            time.sleep(0.1)
            tn = dt.datetime.now()
            if (tn - start).total_seconds() > max_sec:
                break
        assert cc.session_state == SessionStateEnum.Established
        test_logger.debug("test: have client send terminate to server")
        client._readers[0].send_terminate(
            data=bytes(), context=cc, termination_str=" end of test"
        )
        start = dt.datetime.now()
        while cc.session_state != SessionStateEnum.Terminated:
            test_logger.debug(f"client state {str(cc.session_state)} looped")
            time.sleep(0.1)
            tn = dt.datetime.now()
            test_logger.debug(f"client session_state={str(cc.session_state)} {tn}")
            if (tn - start).total_seconds() > max_sec:
                break
        assert cc.session_state == SessionStateEnum.Terminated
        test_logger.debug("join client")
        client._readers_proc[0].join(0.1)
        test_logger.debug("kill client")
        if client._readers_proc[0].is_alive():
            client._readers_proc[0].kill()
        client.stop_all()

    logging.debug("before request_shutdown")
    signal_handler.request_shutdown()
    logging.debug("after request_shutdown")
    # signal_handler.can_run = False

    test_logger.debug("join reaper service")
    rs.join(0.5)
    rs.kill()
    # rs.join()
    if rs.is_alive():
        test_logger.debug("kill reaper service")
        rs.kill()
        time.sleep(0.1)
        if rs.is_alive():
            test_logger.warning("reaper service still running!")
            rs.terminate()
            time.sleep(0.1)
        if rs.is_alive():
            # maybe get the pid and send a signal?
            test_logger.warning(
                f"proc.join(), proc.kill(), and proc.terminate() haven't worked calling 'kill -SIGTERM {rs.pid}'"
            )
            subprocess.Popen(["kill", "-SIGTERM", str(rs.pid)])
            time.sleep(0.1)
        if rs.is_alive():
            # maybe get the pid and send a -9 signal?
            test_logger.warning(
                f"join(), kill(), terminate(), and SIGSTOP haven't worked, calling 'kill -SIGKILL {rs.pid}'"
            )
            subprocess.Popen(["kill", "-SIGKILL", str(rs.pid)])
        if rs.is_alive():
            pytest_pid = os.getpid()
            test_logger.warning(f"kill -SIGKILL {pytest_pid}")
            subprocess.Popen(["kill", "-SIGKILL", str(pytest_pid)])

    logging.debug("at end of trying to kill things, close rs")
    rs.close()
