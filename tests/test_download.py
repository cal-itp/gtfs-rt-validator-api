from tempfile import TemporaryDirectory

from gtfs_rt_validator_api import download_gtfs_schedule_zip, download_rt_files, validate, validate_gcs_bucket
from calitp.storage import get_fs

fs = get_fs()

def test_validation_manual():
    print("downloading data")
    with TemporaryDirectory() as tmp_dir:
        zip_schedule = f"{tmp_dir}/gtfs_schedule"
        dir_rt = f"{tmp_dir}/rt"

        download_gtfs_schedule_zip(
            "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
            zip_schedule,
            fs
        )

        download_rt_files(
            dir_rt,
            fs,
            "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126",
        )

        print("validating")
        validate(zip_schedule, dir_rt)


def test_validate_gcs_bucket():
    validate_gcs_bucket(
        "cal-itp-data-infra", 
        None,
        "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
        "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/2021*/126/0/*",
        out_dir="tests/data/validate_gcs_bucket",
        results_bucket="gs://calitp-py-ci/gtfs-rt-validator-api/test_output"
    )

