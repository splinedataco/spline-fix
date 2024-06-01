from contextlib import contextmanager
from tempfile import TemporaryDirectory
import regex
import os
from pathlib import Path
from typing import cast, Dict
import shutil
import logging

import pytest

# IO_TEST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "io"))
#
SPLINEFIX_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
    )
)

CONFIG_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "config",
        ".config",
        "splinefix",
    )
)

INTEGRATION_TEST_DATA_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "integration_test_data")
)


@pytest.fixture(autouse=True, scope="session")
def config_dir() -> str:
    return CONFIG_DIR


@pytest.fixture(autouse=True, scope="session")
def splinefix_dir() -> str:
    return SPLINEFIX_DIR


@pytest.fixture(autouse=True, scope="session")
def integration_test_data_dir() -> str:
    return INTEGRATION_TEST_DATA_DIR


@pytest.fixture(autouse=True, scope="session")
def gettmpdir() -> Path:
    tmppath = (
        os.environ.get("TMPDIR")
        if os.environ.get("TMPDIR") is not None
        else Path("/") / "tmp"
    )
    return cast(Path, tmppath)


class TestHelpers:
    @staticmethod  # type: ignore[arg-type]
    @contextmanager
    def unique_config(splinefix_dir: str) -> str:  # type: ignore[misc]
        """Take a known location(splinefix_dir) then create a named
        temporary directory
        """
        tmpdir = TemporaryDirectory()
        yaml_regex = regex.compile(
            r"^yaml_config[[:space:]]*=[[:space:]]*.*spline\.yaml$"
        )

        test_dir = Path(splinefix_dir) / "tests" / "utils" / "spline_modules"
        conf_name = str(test_dir / "config.ini")
        spline_yaml = test_dir / Path("spline.yaml")
        newconf_path = Path(tmpdir.name) / "config.ini"
        newyaml_name = str(Path(tmpdir.name) / "spline.yaml")
        with open(file=conf_name, mode="r") as ini:
            with open(file=newconf_path, mode="w+") as newconf:
                for line in ini.readlines():
                    potential_match = yaml_regex.match(line)
                    if potential_match:
                        newconf.write(f"yaml_config = {newyaml_name}\n")
                    else:
                        newconf.write(line)
        shutil.copy(spline_yaml, newyaml_name)
        try:
            yield newconf_path
        finally:
            pass

    @staticmethod  # type: ignore[arg-type]
    @contextmanager
    def idependent_ring_buffers(integration_test_data_dir: str) -> Dict[str, Path]:
        import polars as pl
        import datetime as dt
        from joshua.fix.utils.ring_buffer import WritableRingBuffer
        from joshua.fix.messages.curves import MuniCurvesMessageFactory
        from joshua.fix.messages.predictions import (
            MuniYieldPredictionMessageFactory,
        )

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
        tmpdir = TemporaryDirectory()

        """
        try:
            for feat in features:
                path = curves_20230511700_dir / feat / "curve.parquet"
                logger.debug("test path is: {}".format(path))
                dfs.append(pl.read_parquet(path))
        """
        curves_path = Path(tmpdir.name) / "curves"
        preds_path = Path(tmpdir.name) / "predictions"
        curves_rb = WritableRingBuffer(curves_path, size_in_bytes=2**22)
        preds_rb = WritableRingBuffer(preds_path, size_in_bytes=2**27)
        for dt_tm in [
            dt.datetime(year=2024, month=5, day=23, hour=8, minute=0),
            # dt.datetime(year=2024, month=5, day=23, hour=8, minute=5),
            # dt.datetime(year=2024, month=5, day=23, hour=8, minute=10),
            # dt.datetime(year=2024, month=5, day=23, hour=8, minute=15),
        ]:
            for feat in features:
                try:
                    path = curves_20230511700_dir / feat / "curve.parquet"
                    df = pl.read_parquet(source=path, use_statistics=True)
                    # done one at a time because that's how they appear/get
                    # created in production
                    for c in MuniCurvesMessageFactory.serialize_from_dataframes(
                        dt_tm=dt_tm, feature_sets=[str(feat)], dfs=[df]
                    ):
                        curves_rb.write(data=c)
                except Exception as ex:
                    logging.error(f"Could not read '{path}'. ex: {ex}")

            # and one pred for each time
            try:
                for y in MuniYieldPredictionMessageFactory.serialize_from_dataframe(
                    dt_tm=dt_tm, df=yield_predictions_df
                ):
                    preds_rb.write(y)
            except Exception as ex:
                logging.error(f"failed to serialize prediction. {ex}")

        try:
            yield {
                "curves": curves_path,
                "predictions": preds_path,
            }
        finally:
            pass


@pytest.fixture
def helpers() -> TestHelpers:
    return TestHelpers()
