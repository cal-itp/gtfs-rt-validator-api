"""
NOTE: These tests are deprecated and should be rewritten to not hit GCS.
They will likely not pass in the current state.
"""
import json
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from calitp.storage import get_fs

from gtfs_rt_validator_api import (
    download_gtfs_schedule_zip,
    download_rt_files,
    validate,
    validate_gcs_bucket,
    validate_gcs_bucket_many,
)

# Note that data for these tests is seeded by scripts/seed_bucket_data.py
GCS_BASE_DIR = "gs://calitp-py-ci/gtfs-rt-validator-api"

fs = get_fs()


@pytest.fixture
def tmp_gcs_dir():
    # setup cloud dir for test ----
    dir_name = f"{GCS_BASE_DIR}/test_output/{uuid.uuid4()}"
    fs.mkdir(dir_name)

    yield dir_name

    # clean up by removing directory ----
    # fs.rm(dir_name, recursive=True)


def list_results(dir_name):
    return list(Path(dir_name).glob("*.results.json"))


def test_validation_manual():
    print("downloading data")
    with TemporaryDirectory() as tmp_dir:
        zip_schedule = f"{tmp_dir}/gtfs_schedule"
        dir_rt = f"{tmp_dir}/rt"

        download_gtfs_schedule_zip(
            f"{GCS_BASE_DIR}/gtfs_schedule_126", zip_schedule, fs
        )

        download_rt_files(
            dir_rt, fs, glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/*/126/0/*",
        )

        print("validating")
        validate(zip_schedule, dir_rt)

        assert len(list_results(dir_rt)) > 0


def test_validate_gcs_bucket(tmp_gcs_dir, capsys):

    with TemporaryDirectory() as tmp_dir:
        validate_gcs_bucket(
            "cal-itp-data-infra",
            None,
            f"{GCS_BASE_DIR}/gtfs_schedule_126",
            gtfs_rt_glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/2021*/126/0/*",
            out_dir=tmp_dir,
            results_bucket=tmp_gcs_dir,
        )

    res_files = list(fs.glob(f"{tmp_gcs_dir}/*"))
    assert len(res_files) > 0

    # should not error
    file_with_error = (
        tmp_gcs_dir + "/2021-10-01T00:00:51__126__0__gtfs_rt_service_alerts_url"
    )
    assert "errorMessage" in json.load(fs.open(file_with_error))

    # ensure no error message for parsing timestamp from filename
    captured = capsys.readouterr()
    assert "DateTimeParseException" not in captured.out
    assert "DateTimeParseException" not in captured.err


def test_validate_gcs_bucket_rollup(tmp_gcs_dir):

    with TemporaryDirectory() as tmp_dir:
        validate_gcs_bucket(
            "cal-itp-data-infra",
            None,
            f"{GCS_BASE_DIR}/gtfs_schedule_126",
            gtfs_rt_glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/2021*/126/0/*",
            out_dir=tmp_dir,
            results_bucket=tmp_gcs_dir + "/rollup.parquet",
            aggregate_counts=True,
        )

    fname = f"{tmp_gcs_dir}/rollup.parquet"

    assert fs.exists(fname)

    df = pd.read_parquet(fs.open(fname))
    assert (df.calitp_itp_id == 126).all()

    # 17 distinct rules triggeredrows of data
    assert df.shape[0] == 17


def test_validate_gcs_bucket_many(tmp_gcs_dir):
    with TemporaryDirectory():
        validate_gcs_bucket_many(
            "cal-itp-data-infra",
            None,
            "gs://calitp-py-ci/gtfs-rt-validator-api/validation_params.csv",
            results_bucket=tmp_gcs_dir,
            verbose=True,
            aggregate_counts=True,
            summary_path=tmp_gcs_dir + "/status.json",
            strict=True,
        )

    fname = f"{tmp_gcs_dir}/validation_results/126/0/everything.parquet"
    df = pd.read_parquet(fs.open(fname))

    assert (df.calitp_itp_id == 126).all()

    # 17 distinct rules triggered
    assert df.shape[0] == 17

    status = pd.read_json(tmp_gcs_dir + "/status.json", lines=True)
    assert len(status) == 1
    assert status["is_success"].eq(True).all()


def test_validate_gcs_bucket_many_25(tmp_gcs_dir):
    with TemporaryDirectory():
        validate_gcs_bucket_many(
            "cal-itp-data-infra",
            None,
            "gs://calitp-py-ci/gtfs-rt-validator-api/validation_params_many.csv",
            results_bucket=tmp_gcs_dir,
            verbose=True,
            aggregate_counts=True,
            summary_path=tmp_gcs_dir + "/status.json",
            threads=4,
        )

    fs = get_fs()
    gcs_files = fs.glob(tmp_gcs_dir + "/validation_results/*/*/everything.parquet")

    # check that bucket contains the 24 rollups and a status.json
    assert len([x for x in gcs_files if "everything" in x]) == 24

    # check that 1 failed feed is in status
    status = pd.read_json(tmp_gcs_dir + "/status.json", lines=True)
    assert len(status) == 25
    assert len(status[~status.is_success]) == 1

    # check 1 result file
    fname = f"{tmp_gcs_dir}/validation_results/106/0/everything.parquet"
    df = pd.read_parquet(fs.open(fname))

    assert (df.calitp_itp_id == 106).all()
