import uuid
import pytest
import json

from tempfile import TemporaryDirectory

from gtfs_rt_validator_api import download_gtfs_schedule_zip, download_rt_files, validate, validate_gcs_bucket
from calitp.storage import get_fs
from pathlib import Path

# Note that data for these tests is seeded by scripts/seed_bucket_data.py
GCS_BASE_DIR="gs://calitp-py-ci/gtfs-rt-validator-api"

fs = get_fs()

@pytest.fixture
def tmp_gcs_dir():
    # setup cloud dir for test ----
    dir_name = f"{GCS_BASE_DIR}/test_output/{uuid.uuid4()}"
    fs.mkdir(dir_name)

    yield dir_name

    # clean up by removing directory ----
    fs.rm(dir_name, recursive=True)


def list_results(dir_name):
    return list(Path(dir_name).glob("*.results.json"))


def test_validation_manual():
    print("downloading data")
    with TemporaryDirectory() as tmp_dir:
        zip_schedule = f"{tmp_dir}/gtfs_schedule"
        dir_rt = f"{tmp_dir}/rt"

        download_gtfs_schedule_zip(
            f"{GCS_BASE_DIR}/gtfs_schedule_126",
            zip_schedule,
            fs
        )

        download_rt_files(
            dir_rt,
            fs,
            glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/*/126/0/*",
        )

        print("validating")
        validate(zip_schedule, dir_rt)

        assert len(list_results(dir_rt)) > 0


def test_validate_gcs_bucket(tmp_gcs_dir):

    with TemporaryDirectory() as tmp_dir:
        validate_gcs_bucket(
            "cal-itp-data-infra", 
            None,
            f"{GCS_BASE_DIR}/gtfs_schedule_126",
            gtfs_rt_glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/2021*/126/0/*",
            out_dir=tmp_dir,
            results_bucket=tmp_gcs_dir
        )

    res_files = list(fs.glob(f"{tmp_gcs_dir}/*"))
    assert len(res_files) > 0
    
    # should not error
    assert "errorMessage" in json.load(fs.open(res_files[0]))


def test_validate_gcs_bucket_rollup(tmp_gcs_dir):
    import pandas as pd

    with TemporaryDirectory() as tmp_dir:
        validate_gcs_bucket(
            "cal-itp-data-infra", 
            None,
            f"{GCS_BASE_DIR}/gtfs_schedule_126",
            gtfs_rt_glob_path=f"{GCS_BASE_DIR}/gtfs_rt_126/2021*/126/0/*",
            out_dir=tmp_dir,
            results_bucket=tmp_gcs_dir + "/rollup.parquet",
            aggregate_counts=True
        )

    fname = f"{tmp_gcs_dir}/rollup.parquet"
    
    assert fs.exists(fname)
    
    assert (pd.read_parquet(fs.open(fname)).calitp_itp_id == 126).all()
