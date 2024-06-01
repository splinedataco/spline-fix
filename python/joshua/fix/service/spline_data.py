from typing import Any, cast, Dict, List, Optional, Tuple, Union
import sys
from enum import Enum
import multiprocessing as mp
from multiprocessing import Process
from multiprocessing.connection import Connection, Listener, Pipe
from multiprocessing.sharedctypes import Synchronized
import logging
import signal
import socket
import time
import datetime as dt
from pathlib import Path
import ctypes


from joshua.fix.fixp.messages import Jwt
from joshua.fix.server import FixServerSession
from joshua.fix.reader import SessionStateEnum

start_method = mp.get_start_method()
if start_method is None:
    mp.set_start_method("forkserver")
mp_ctx = mp.get_context(mp.get_start_method())
# logger = mp_ctx.log_to_stderr()
logger = mp_ctx.get_logger()

# logger.setLevel(logging.root.level)
logger.setLevel(logging.WARNING)


class SignalHandler:
    # shutdown_requested = False
    _shutdown_requested: bool  # Synchronized[bool]

    def __init__(self) -> None:
        self.original_sig_int = signal.getsignal(signal.SIGINT)
        self.original_sig_term = signal.getsignal(signal.SIGTERM)

        signal.signal(signal.SIGINT, self.request_shutdown)
        # signal.signal(signal.SIGUSR1, self.request_shutdown)
        signal.signal(signal.SIGTERM, self.request_shutdown)
        self._shutdown_requested = cast(
            Synchronized, mp.sharedctypes.Value(ctypes.c_bool, False, lock=True)  # type: ignore[assignment]
        )

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested.value  # type: ignore[attr-defined]

    @shutdown_requested.setter
    def shutdown_requested(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise NotImplementedError
        self._shutdown_requested.value = value  # type: ignore[attr-defined]

    def request_shutdown(self, *args) -> None:  # type: ignore[no-untyped-def]
        try:
            # now that we've set our flag and will never reset, allow
            # other signals to do their thing
            logging.warning("Received shutdown request")
            print("Received shutdown request")
        except Exception as ex:
            logging.error(f"Error trying to log shutdown request. {ex}")
        finally:
            self.shutdown_requested = True
            signal.signal(signal.SIGINT, self.original_sig_int)
            signal.signal(signal.SIGTERM, self.original_sig_term)

    @property
    def can_run(self) -> bool:
        return not self.shutdown_requested

    @can_run.setter
    def can_run(self, value: bool) -> None:
        if not isinstance(value, bool):
            raise NotImplementedError
        self.shutdown_requested = value


class Action(Enum):
    SpawnSession = 0
    StopAll = 1


class Result(Enum):
    Error = 0
    Ok = 1


def send_pipe(conn: Connection, data: Any) -> bool:
    try:
        logger.debug("send_pipe: {}".format(data))
        logger.debug(f"conn.closed={conn.closed} and con.writable={conn.writable}")
        if not conn.closed and conn.writable:
            conn.send(data)
            return True
    except Exception as ex:
        logger.warning(
            "pipe write between spawn reaper and server raised exception: {}".format(ex)
        )
    return False


def read_pipe(conn: Connection) -> Optional[Tuple[Action, Any]]:
    try:
        if not conn.closed and conn.readable and conn.poll(0.1):
            logging.debug("polled is True")
            (cmd, data) = conn.recv()
            logging.debug(f"read: {(cmd, data)} from pipe")
            return (cmd, data)
    except Exception as ex:
        logging.warning(
            "pipe read between spawn reaper and server raised exception: {}".format(ex)
        )
    return None


def spawn_reap_loop(
    bidir: Connection, context: Dict[str, Any], signal_handler: SignalHandler
) -> None:
    """Expects a tuple of (Action, data) to be retrieved
    from the pipe. Responses and status are sent in the same format.
    """
    # signal_handler = SignalHandler()
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    # spawn_logger = mp_ctx.log_to_stderr()
    spawn_logger = mp_ctx.get_logger()

    # spawn_logger.setLevel(logging.root.level)
    # spawn_logger.setLevel(logging.getLogger().level)
    spawn_logger.setLevel(logging.WARNING)
    spawn_logger.propagate = True
    spawn_logger.debug(
        f"spawn_logger: spawn reaper. signal_handler: {signal_handler} sh.can_run={signal_handler.can_run}"
    )
    ring_buffer_prefix = context["ring_buffer_prefix"]
    if ring_buffer_prefix is None:
        ring_buffer_prefix = "~/var"

    # logging.debug("logging: spawn reaper")
    sr = SpawnReaper()
    """
    spawn_logger.debug(
        f"before: reap_loop read_pipe active_sessions: {len(sr.active_sessions)} and can_run: {signal_handler.can_run}"
    )
    """
    while signal_handler.can_run:
        """
        spawn_logger.debug(
            f"reap_loop read_pipe active_sessions: {len(sr.active_sessions)} and can_run: {signal_handler.can_run}"
        )
        """
        try:
            from_parent = read_pipe(conn=bidir)
        except Exception as ex:
            spawn_logger.critical(f"error returned from read_pipe: {ex}")
            raise ex
        # spawn_logger.debug(f"reaper:spawn_logger: read_pipe -> {from_parent}")
        # logging.debug(f"reaper:logging: read_pipe -> {from_parent}")
        # print(f"reaper:print: read_pipe -> {from_parent}")
        if from_parent is not None:
            spawn_logger.debug(f"from_parent: {from_parent}")
            (cmd, data) = from_parent
            match cmd:
                case Action.SpawnSession:
                    spawn_logger.debug("Action.SpawnSession call start_session")
                    logging.debug("Action.SpawnSession call start_session")
                    print("Action.SpawnSession call start_session")
                    (conn, heartbeat_ms) = data
                    sr.start_session(
                        conn=conn,
                        heartbeat_ms=heartbeat_ms,
                        ring_buffer_prefix=ring_buffer_prefix,
                    )
                    # ret_data = (Result.Ok, (cmd, data))
                    ret_data: Tuple[Result, str] = (Result.Ok, "")
                    spawn_logger.debug(f"calling_send_pipe(data={ret_data})")
                    ret_bool = send_pipe(conn=bidir, data=ret_data)
                    if not ret_bool:
                        spawn_logger.error(
                            f"could not send {ret_data} to parent process"
                        )
                        raise Exception("could not send result to parent")
                    spawn_logger.debug("send_pipe was successful")
                case Action.StopAll:
                    spawn_logger.debug("Got Action.StopAll")
                    # make sure that terminate flag is set
                    signal_handler.request_shutdown()
                    signal_handler.can_run = False
                    print("signal_handler.can_run = False")
                    sr.kill_em_all()
                    spawn_logger.debug("returned from kill_em_all")
                    remaining = sr.active_sessions
                    spawn_logger.debug(f"remaining active sessions: {remaining}")
                    if len(remaining) > 0:
                        ret_data = (
                            Result.Error,
                            str(
                                f"unable to stop all processes. {len(remaining)} remain"
                            ),
                        )
                        if not send_pipe(conn=bidir, data=ret_data):
                            spawn_logger.error(
                                f"could not send {ret_data} to parent process"
                            )
                    else:
                        # none remain
                        ret_data = (Result.Ok, str((cmd, data)))
                        if not send_pipe(conn=bidir, data=ret_data):
                            spawn_logger.error(
                                f"could not send {ret_data} to parent process"
                            )
        # else: nothing to read
        # then: either read or did not, do housekeeping
        # logging.debug("call clean_sessions")
        num_reaped = sr.clean_sessions()
        if num_reaped > 0:
            spawn_logger.debug(f"reaped {num_reaped} sessions")
        # delay a small amount so processor isn't in 100% busy wait
        time.sleep(0.1)


class SpawnReaper:
    _active_sessions: List[FixServerSession] = []

    def __init__(self) -> None:
        start_method = mp.get_start_method()
        if start_method is None:
            mp.set_start_method("forkserver")
        mp_ctx = mp.get_context(mp.get_start_method())
        # self.logger = mp_ctx.log_to_stderr()
        self.logger = mp_ctx.get_logger()
        self.logger.propagate = False

        # logger.setLevel(logging.root.level)
        self.logger.setLevel(logging.getLogger().level)

        self._active_sessions = []

    @property
    def active_sessions(self) -> List[FixServerSession]:
        return self._active_sessions

    @active_sessions.setter
    def active_sessions(self, value: List[FixServerSession]) -> None:
        if not isinstance(value, list):
            raise NotImplementedError
        self._active_sessions = value

    def clean_sessions(self) -> int:
        # self.logger.debug("cleaning sessions")
        start_num = len(self._active_sessions)
        still_active = [
            s
            for s in self.active_sessions
            if s._readers[0].session_state != SessionStateEnum.Terminated
        ]
        num_removed = start_num - len(still_active)
        self._active_sessions = still_active
        """
        self.logger.debug(
            f"removed {num_removed} terminated sessions. {len(still_active)} remain."
        )
        """
        return num_removed

    def kill_em_all(self) -> None:
        self.logger.warning("kill_em_all called")
        start_time = dt.datetime.now()
        while len(self.active_sessions) > 0:
            for s in self.active_sessions:
                if (
                    s._readers[0].session_state != SessionStateEnum.Terminate
                    and s._readers[0].session_state != SessionStateEnum.Terminating
                    and s._readers[0].session_state != SessionStateEnum.Terminated
                ):
                    s.stop_all()

            # keep only the sessions that are not terminated
            self.active_sessions = [
                s
                for s in self.active_sessions
                if s._readers[0].session_state != SessionStateEnum.Terminated
            ]
            # 90 seconds is 3x30 seconds. And 30 seconds is the longest heartbeat we allow
            if (dt.datetime.now() - start_time).total_seconds() > 90:
                return
            time.sleep(0.1)
        # if here sessions is empty and we're done
        self.logger.warning("kill_em_all finished")
        return

    def start_session(
        self,
        conn: Connection,
        heartbeat_ms: int = 1000,
        ring_buffer_prefix: Optional[Union[str, Path]] = None,
    ) -> FixServerSession:
        """Given a connection creates a FixServerSession and adds to
        the active list. Provides capability to manage sessions from one place.
        """
        try:
            if ring_buffer_prefix is None:
                ring_buffer_prefix = "~/var/"
            ring_buffer_prefix = Path(ring_buffer_prefix).expanduser().resolve()
            self.logger.debug("start_session called")
            # make sure that our socket has the options we want
            conn_sock = socket.socket(fileno=conn.fileno())
            # send immediately, don't wait to aggregate
            # zero=False non-zero=True
            conn_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # if we shutdown ensure we can immediately restart
            conn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            conn_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            # spawns in a new process
            self.logger.debug(
                f"spawn new fix session. Setting ring_buffer_path={ring_buffer_prefix}"
            )
            fix_session = FixServerSession(
                conn=conn,
                heartbeat_ms=heartbeat_ms,
                ring_buffer_path=ring_buffer_prefix,
            )
            self.logger.debug("append new fix session: {}".format(fix_session))
            self.active_sessions.append(fix_session)
            self.logger.debug(
                "reaper.start_session: len(active_sessions)={}".format(
                    len(self.active_sessions)
                )
            )
            return fix_session
        except Exception as ex:
            self.logger.error(f"unable to start_session: {ex}")
            raise ex


def running_start(cmd_ctx: Dict[str, Any], signal_handler: SignalHandler) -> None:
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    # logger = mp_ctx.log_to_stderr()
    logger = mp_ctx.get_logger()
    # logger.setLevel(logging.getLogger().level)
    logger.setLevel(logging.WARNING)
    logger.propagate = False

    # logger.setLevel(logging.root.level)
    # logger.setLevel(logging.getLogger().level)
    """
    signal_handler = SignalHandler()
    logger.debug(f"just created signal_handler: {signal_handler}")
    logger.debug(f"just created signal_handler.can_run: {signal_handler.can_run}")
    """

    logger.debug("spline_data.running_start called")
    address = cmd_ctx.get("ipv4addr")
    if address is None:
        # binding to all addresses with SO_REUSEADDR can allow an attacker on the host
        # to open a port on the same socket/port and steal the traffic.
        # recommended to navigate through all addresses and add each individually.
        # TODO: enumerate all local addresses and listen on each individually.
        # will need to accept from all of them.
        address = "0.0.0.0"  # nosec - B108 binding to all interfaces. - [B104:hardcoded_bind_all_interfaces]
    port = cmd_ctx.get("port")
    if port is None:
        port = 6000
    work_dir = cmd_ctx.get("work_dir")
    if work_dir is None:
        work_dir = "~/s3"
    heartbeat_ms = cmd_ctx.get("heartbeat_interval")
    if heartbeat_ms is None:
        heartbeat_ms = 1000

    ipv4addr_port = (address, port)
    logger.info(f"starting server with address: {ipv4addr_port}")

    sl, s = Pipe(duplex=True)
    logger.info("spawn reaper process")
    spawnloop = Process(
        target=spawn_reap_loop,
        args=(s,),
        kwargs={"context": cmd_ctx, "signal_handler": signal_handler},
        daemon=False,
    )
    spawnloop.start()
    logger.info("started spawn reaper process")
    # close our copy of the other side
    s.close()

    print(f"with Listener({ipv4addr_port}) as listener:")
    with Listener(ipv4addr_port) as listener:
        logger.debug("server listening")
        print("server listening")
        if not signal_handler.can_run:
            print("not signal_handler.can_run: received shutdown request")
            logger.warning("received shutdown request")
            logger.warning("service: send_pipe(data=(Action.StopAll, None))")
            print("service: send_pipe(data=(Action.StopAll, None))")
            if not send_pipe(conn=sl, data=(Action.StopAll, None)):
                logger.critical("Error stopping sessions.")
                raise Exception("unable to send message to reaper")
            # want to try to get the immediate response
            if not sl.closed and sl.poll(0.1):
                (status, data) = sl.recv()
                if status != Result.Ok:
                    if status != Result.Error:
                        logger.critical(
                            "status result from reaper unconventional error: {status} data: {data}."
                        )
                        # raise NotImplementedError
                    logger.error(f"Error trying to reap all sessions. {data}")
                    (msg, num) = data
                    # return num
            if spawnloop.is_alive():
                logging.warning("reaper: spawnloop.is_alive. try to join(1)")
                spawnloop.join(1)
            if spawnloop.is_alive():
                logging.warning("reaper: spawnloop.is_alive. try to kill()")
                spawnloop.kill()
            logging.warning("reaper: spawnloop try to close()")
            spawnloop.close()
            return
        logger.debug("check while signal_handler.can_run")
        while signal_handler.can_run:
            logger.debug("with listener.accept() as conn")
            # blocks
            conn = listener.accept()

            logger.debug("with listener.accept() as conn:")
            logger.info(f"connection accepted from, {listener.last_accepted}")
            logger.debug(
                "service: send_pipe(data=(Action.SpawnSession, (conn, heartbeat_ms)))"
            )
            if not send_pipe(conn=sl, data=(Action.SpawnSession, (conn, heartbeat_ms))):
                logger.error(
                    "Failed to send Action.SpawnSession to send_pipe. Shutting down."
                )
                if spawnloop.is_alive():
                    spawnloop.join(1)
                if spawnloop.is_alive():
                    spawnloop.kill()
                spawnloop.close()

                logger.critical("Error stopping sessions.")
                raise Exception("unable to send message to reaper")
            # want to try to get the immediate response
            logger.debug(f"check not sl.closed={sl.closed} and sl.poll(0.1)")
            try:
                if not sl.closed and sl.poll(0.1):
                    logger.debug(
                        "recv from pipe: sl.closed == False sl.poll(0.1) is not None"
                    )
                    logger.debug("receive status")
                    (status, data) = sl.recv()
                    logger.debug(f"received: status=({status}, {data})")
                    if status != Result.Ok:
                        logger.error(
                            f"got non ok status from recv. status=({status}, {data})"
                        )
                        if status != Result.Error:
                            logger.critical(
                                f"status result from reaper is unconventional. status=({status}, {data})"
                            )
                            raise NotImplementedError
                        logger.error(f"Error to create new session. {data}")
                        (msg, num) = data
                        if spawnloop.is_alive():
                            spawnloop.join(1)
                        if spawnloop.is_alive():
                            spawnloop.kill()
                        spawnloop.close()

                        raise Exception(f"Did not get message from read_pipe. {data}")
            except Exception as ex:
                logger.error(f"error trying to read from control pipe. ex: {ex}")
            logger.debug("end of server accept while loop, looping")
        logger.warning("service: send_pipe(data=(Action.StopAll, None))")
        if not send_pipe(conn=sl, data=(Action.StopAll, None)):
            if spawnloop.is_alive():
                spawnloop.join(1)
            if spawnloop.is_alive():
                spawnloop.kill()
            spawnloop.close()

            logger.critical("Error stopping sessions.")
            raise Exception("unable to send message to reaper")
        # want to try to get the immediate response
        if not sl.closed and sl.poll(1.0):
            (status, data) = sl.recv()
            if status != Result.Ok:
                if status != Result.Error:
                    logger.critical(
                        "status result from reaper is unconventional. Exiting."
                    )
                    raise NotImplementedError
                logger.error(f"Error trying to reap all sessions. {data}")
                (msg, num) = data
                if spawnloop.is_alive():
                    spawnloop.join(1)
                if spawnloop.is_alive():
                    spawnloop.kill()
                spawnloop.close()

                return

    spawnloop.join(1.0)
    if spawnloop.is_alive():
        spawnloop.kill()
    spawnloop.close()
    sl.close()


if __name__ == "__main__":
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.log_to_stderr()
    # logger = mp_ctx.get_logger()
    # logger.setLevel(logging.getLogger().level)
    logger.setLevel(logging.WARNING)
    logger.propagate = True

    cmd_ctx = {
        "ipv4addr": "127.0.0.1",
        "port": 36000,
        "work_dir": "~/s3",
        "heartbeat_interval": 1000,
        "ring_buffer_prefix": "~/var/",
    }
    rs = Process(
        target=running_start,
        name="spline_data_running_start",
        args=((cmd_ctx),),
        daemon=False,
    )
    rs.start()
    logger.info("server started: cmd_ctx: {}".format(cmd_ctx))
    logger.info("default_jwt:\n{}".format(Jwt()))
    signal_handler = SignalHandler()
    while signal_handler.can_run:
        sys.stdout.flush()
        time.sleep(1.0)
        if not rs.is_alive():
            logger.critical("service Process is not alive")
            exit(1)
    logger.warning("exited main loop. can_run=False")
    rs.join(1.0)
    rs.kill()
    time.sleep(0.1)
    rs.close()
