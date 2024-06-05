from __future__ import annotations
from functools import cached_property

import multiprocessing as mp
from typing import Any, cast, Dict, Optional, Tuple, Union
import logging
import time
import polars as pl
from pathlib import Path
import regex
import datetime as dt
import os

from watchdog.events import (
    FileSystemEvent,
    RegexMatchingEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
)
from watchdog.observers import Observer

from joshua.fix.utils.ring_buffer import WritableRingBuffer
from joshua.fix.messages.featureset import FeatureSet
from joshua.fix.messages.curves import MuniCurvesMessageFactory
from joshua.fix.messages.predictions import MuniYieldPredictionMessageFactory


# need for nfs, cifs, or other networked filesystem
# Will get chosen by Observer
from watchdog.observers.polling import PollingObserver

loglevel = logging.getLogger().level


class WatchThis:
    watched_path: Path

    def __init__(self, path: Union[Path, str]) -> None:
        if path is None:
            raise NotImplementedError
        self._watched_path = Path(path)
        self.observer = Observer()

    def run(self) -> None:
        event_handler = Handler(
            regexes=[r".*\.parquet"], ignore_regexes=[r".*\.csv", r".*\.log"]
        )
        evtfilt = [FileCreatedEvent, FileModifiedEvent]
        self.observer.schedule(
            event_handler=event_handler,
            path=self.watched_path,
            recursive=True,
            event_filter=evtfilt,
        )


class Handler(RegexMatchingEventHandler):
    def __init__(  # type: ignore[no-untyped-def]
        self,
        regexes=None,
        ignore_regexes=None,
        ignore_directories=False,
        case_sensitive=False,
        product_type: str = "curves",
        path_prefix: Union[Path, str] = "~/var/",
    ) -> None:
        super().__init__(
            regexes=regexes,
            ignore_regexes=ignore_regexes,
            ignore_directories=ignore_directories,
            case_sensitive=case_sensitive,
        )
        start_method = mp.get_start_method()
        if start_method is None:
            mp.set_start_method("forkserver")
        mp_ctx = mp.get_context(mp.get_start_method())
        self.logger = mp_ctx.get_logger()
        self.logger.propagate = True
        # logger.setLevel(logging.root.level)
        # self.logger.setLevel(level=logging.DEBUG)
        self.logger.setLevel(level=logging.getLogger().level)
        self.logger.debug(
            f"type(product_type)={type(product_type)} product_type={product_type}"
        )
        self.product = product_type
        self.file_prefix = Path(path_prefix).expanduser().resolve()
        self.logger.debug(f"{self.file_prefix}.exists()={self.file_prefix.exists()}")
        if not self.file_prefix.exists():
            self.file_prefix.mkdir(parents=True, exist_ok=True)
        if not self.file_prefix.is_dir():
            self.logger.error(f"{self.file_prefix} must be a directory")
            raise ValueError(f"{self.file_prefix} must be a directory")
        self._write_buffer: Optional[WritableRingBuffer] = None
        self.featureset_regex = regex.compile(
            r".*([abc]{1,})_(go|rev)_([[:digit:]]{1,2})_(lrg|med|sml)_(lqd|ilqd).*"
        )
        self.date_time_regex = regex.compile(
            r".*([[:digit:]]{4,4})/([[:digit:]]{2,2})/([[:digit:]]{2,2})/([[:digit:]]{2,2})/([[:digit:]]{2,2}).*"
        )
        self.logger.debug(f"before open {self.product}")
        self.open()
        self.logger.debug(f"after open {self.product}")

    def write_buffer(self) -> WritableRingBuffer:
        if self._write_buffer is None:
            raise ValueError("write buffer has no value")
        return cast(WritableRingBuffer, self._write_buffer)

    def get_regex_parts(
        self, path: Union[Path, str]
    ) -> Tuple[dt.datetime, Optional[FeatureSet]]:
        self.logger.debug(f"{self.product}.get_regex_parts: {path}")
        path = str(path)
        dt_tm_match = self.date_time_regex.match(path)
        if dt_tm_match is None:
            self.logger.error(f"{path} is not in a curve or prediction format")
            raise ValueError(f"{path} is not in a curve or prediction format")
        dt_tm_groups = dt_tm_match.groups()
        dt_tm = dt.datetime(
            year=int(dt_tm_groups[0]),
            month=int(dt_tm_groups[1]),
            day=int(dt_tm_groups[2]),
            hour=int(dt_tm_groups[3]),
            minute=int(dt_tm_groups[4]),
        )

        fs: Optional[FeatureSet]
        if self.is_curve:
            fs = FeatureSet.from_str(path)
        else:
            fs = None

        self.logger.debug(f"get_regex_parts: returning {(dt_tm, fs)}")
        return (dt_tm, fs)

    @property
    def product(self) -> str:
        return self._product

    @product.setter
    def product(self, name: str) -> None:
        if name is None:
            raise NotImplementedError
        self._product = name

    @cached_property
    def is_curve(self) -> bool:
        return self.product.startswith("curve")

    @cached_property
    def is_prediction(self) -> bool:
        return self.product.startswith("prediction")

    @property
    def file_prefix(self) -> Path:
        return self._file_prefix

    @file_prefix.setter
    def file_prefix(self, path: Union[Path, str]) -> None:
        if path is None:
            raise NotImplementedError
        self._file_prefix = Path(path)

    def file_path(self) -> Path:
        return Path((self.file_prefix / self.product).expanduser()).resolve()

    def pos_file_path(self) -> Path:
        return Path(self.file_path().with_suffix(".pos").expanduser()).resolve()

    def read_and_write_change(self, path: Union[Path, str]) -> int:
        """TODO: add a Queue into the emitter/dispatch that pushes the new entry
        into the queue rather than immediately processing. Then have another thread
        monitor the queue on an interval. Take all entries and send them in a batch
        rather than one at a time. Predicions will always be in a batch. Curves
        will be 1 at a time.
        """
        dt_tm, fs = self.get_regex_parts(path)
        self.logger.debug(f"read parquet({path})")
        try:
            df = pl.read_parquet(source=path, use_statistics=True)
        except Exception as ex:
            self.logger.error(f"Could not read '{path}'. ex: {ex}")
            return 0
        self.logger.debug(f"parquet read:\n{df.head(10)}")
        total_written: int = 0
        if self.is_curve:
            self.logger.debug("is_curve serialize_from_dataframes")
            try:
                for c in MuniCurvesMessageFactory.serialize_from_dataframes(
                    dt_tm=dt_tm,
                    feature_sets=[str(fs)],
                    dfs=[df],
                ):
                    self.logger.debug(
                        "curves serialize_from_dataframe(df) returned:\n{}".format(
                            c.hex(":")
                        )
                    )
                    total_written += self.write_buffer().write(c)
            except Exception as ex:
                self.logger.error(f"failed to serialize curves. {ex}")
            finally:
                self.logger.debug(
                    f"write curves returning total_written={total_written}"
                )
            return total_written

        elif self.is_prediction:
            try:
                # will only have one ring buffer, and it should be for the type we're monitoring
                self.logger.debug(
                    "is_prediction MuniYieldPredictionMessageFactory serialize_from_dataframes"
                )
                for y in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
                    dt_tm=dt_tm, df=df
                ):
                    self.logger.debug(
                        "predictions serialize_from_dataframe(df) returned:\n{}".format(
                            y.hex(":")
                        )
                    )
                    total_written += self.write_buffer().write(y)
                    self.logger.debug(f"prediction total_written={total_written}")
            except Exception as ex:
                self.logger.error(f"failed to serialize prediction. {ex}")
            finally:
                self.logger.debug(
                    f"write predictions returning total_written={total_written}"
                )
            return total_written
        return total_written

    def on_created(self, event: FileSystemEvent) -> None:
        self.logger.debug("$" * 50)
        super().on_created(event)
        self.logger.debug(f"on_created: {event}")
        if event.is_directory:
            self.logger.debug(
                f"{event.src_path} is a directory. dir should be {self.product}"
            )
            return
        self.logger.debug(f"{event.src_path} was created. log should be {self.product}")
        try:
            bytes_out = self.read_and_write_change(path=event.src_path)
            self.logger.debug(
                f"on_created wrote out {bytes_out} bytes for {event.src_path}"
            )
        except ValueError as ex:
            self.logger.debug(f"Exception processing, continuing. ex={ex}")
            return

    def on_modified(self, event: FileSystemEvent) -> None:
        self.logger.debug("-" * 50)
        super().on_modified(event)
        self.logger.debug(f"on_modified: {event}")
        if event.is_directory:
            self.logger.debug(f"{event.src_path} is a directory")
            return
        self.logger.debug(
            f"{event.src_path} was modified. log should be {self.product}"
        )
        try:
            bytes_out = self.read_and_write_change(path=event.src_path)
            self.logger.debug(
                f"on_modified wrote out {bytes_out} bytes for {event.src_path}"
            )
        except ValueError as ex:
            self.logger.debug(f"Exception processing, continuing. ex={ex}")
            return

    def on_moved(self, event: FileSystemEvent) -> None:
        self.logger.debug("*" * 50)
        super().on_moved(event)
        self.logger.debug(f"on_moved: {event}")
        if event.is_directory:
            print(f"{event.src_path} is a directory")
            return
        print(
            f"{event.src_path} was moved to {event.dest_path}. Should be {self.product}"
        )
        self.logger.debug(
            f"{event.src_path} was moved to {event.dest_path}. log should be {self.product}"
        )
        try:
            bytes_out = self.read_and_write_change(path=event.dest_path)
            self.logger.debug(
                f"on_modified wrote out {bytes_out} bytes for {event.dest_path}"
            )
        except ValueError as ex:
            self.logger.debug(f"Exception processing, continuing. ex={ex}")
            return

    def open(self) -> None:
        self.logger.debug(f"open({self.file_path()})")
        if self._write_buffer is not None:
            del self._write_buffer
        size = 0
        if self.is_prediction:
            # 134MiB
            size = 2**27
        elif self.is_curve:
            # 32MiB
            size = 2**25
        self._write_buffer = WritableRingBuffer(
            filename=self.file_path(), size_in_bytes=size
        )
        self.logger.debug(
            f"opened({self.file_path()} self._write_buffer._file_mmap.closed()=={self._write_buffer._file_mmap.closed}"
        )


def main() -> int:
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = mp_ctx.get_logger()
    logger.propagate = False
    # logger.setLevel(logging.root.level)
    logger.setLevel(logging.WARNING)
    # set the format for logging info
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    today = dt.date.today()
    cmd_ctx = {
        "active_bucket_name": "splineactive",
        "shared_bucket_name": "splineshared",
        "curves_prefix": "curves",
        "weights_prefix": "curve_weights",
        "trades_prefix": "msrbrtrs",
        "work_dir": "~/test_s3",
        "endpoint_url": "https://s3.us-east-2.amazonaws.com",
        "region_name": "us-east-2",
        "access_key_id": os.getenv("SPLINE_ACCESS_KEY_ID", "baddefault"),
        "access_key": os.getenv("SPLINE_ACCESS_KEY", "stillbaddefault"),
        "tradedate": today.strftime("%Y%m%d"),
        "logLevel": "WARNING",
    }
    logger.setLevel(cmd_ctx["logLevel"])
    return publish_main(cmd_ctx)


def publish_main(cmd_ctx: Dict[str, Any]) -> int:
    start_method = mp.get_start_method()
    if start_method is None:
        mp.set_start_method("forkserver")
    mp_ctx = mp.get_context(mp.get_start_method())
    logger = cast(logging.Logger, cmd_ctx.get("logger"))
    if logger is None:
        logger = mp_ctx.get_logger()
    for name, logg in logging.root.manager.loggerDict.items():
        if name.startswith("spline"):
            logger = cast(logging.Logger, logg)
            break
    logger = cast(logging.Logger, logging.getLogger(f"{logger.name}.publish_fix"))
    logger.propagate = False
    logger.setLevel(cmd_ctx["loglevel"])

    logger.debug("starting publisher")
    logger.debug("initial_config:\n{}".format(cmd_ctx))

    # Set format for displaying path
    # path = sys.argv[1] if len(sys.argv) > 1 else "."
    combined_predictions_prefix: str
    curves_prefix: str
    combined_predictions_prefix = "combined/predictions"
    curves_prefix = "curves"
    work_dir = "~/work"
    tradedate_to_monitor = cmd_ctx["tradedate"]

    predictions_path = (
        (
            Path(work_dir)
            / combined_predictions_prefix
            / "predictions"
            / tradedate_to_monitor.strftime("%Y")
            / tradedate_to_monitor.strftime("%m")
            / tradedate_to_monitor.strftime("%d")
        )
        .expanduser()
        .resolve()
    )
    curves_path = (
        (
            Path(work_dir)
            / curves_prefix
            / tradedate_to_monitor.strftime("%Y")
            / tradedate_to_monitor.strftime("%m")
            / tradedate_to_monitor.strftime("%d")
        )
        .expanduser()
        .resolve()
    )

    # make sure the directories exist
    curves_path.mkdir(parents=True, exist_ok=True)
    predictions_path.mkdir(parents=True, exist_ok=True)
    logger.debug(f"paths to monitor =\n{curves_path}\n{predictions_path}")

    # Initialize logger event handler
    logger.debug("start Handler(path)")
    curves_event_handler = Handler(
        regexes=[r".*\.parquet$"],
        # ignore_regexes=[r".*\.minio"],
        # ignore_directories=True,
        product_type="curves",
        path_prefix="~/var",
    )
    predictions_event_handler = Handler(
        regexes=[r".*\.parquet$"],
        # ignore_regexes=[r".*\.minio"],
        ignore_directories=True,
        product_type="predictions",
        path_prefix="~/var",
    )

    # Initialize Observer
    logger.debug("create Observer()")
    # observer = Observer()
    observer = PollingObserver()
    logger.debug("observer.schedule(event_handler, path, recursive)")
    observer.schedule(curves_event_handler, curves_path, recursive=True)
    observer.schedule(predictions_event_handler, predictions_path, recursive=True)
    logger.debug(
        f"predictions_event_handler == curves_event_handler {predictions_event_handler == curves_event_handler}"
    )
    logger.debug(
        f"predictions_event_handler == predictions_event_handler {predictions_event_handler == predictions_event_handler}"
    )

    # Start the observer
    logger.debug("start the observer")
    observer.start()
    logger.debug(f"observer: {observer.emitters}")
    # logger.debug(f"path: {path}")
    try:
        while True:
            logger.debug("sleep(1)")
            # Set the thread sleep time
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
