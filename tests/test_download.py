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


#validate_gcs_bucket(
#    "cal-itp-data-infra",
#    None,
#    "gs://gtfs-data/schedule/2021-09-01T00:00:00+00:00/106_0",
#    gtfs_rt_glob_path="gs://gtfs-data/rt/2021-09-01T*/106/0/*",
#
#    # uncomment to push the resulting validation files up to a gcs bucket.
#    # note that the validator produces 1 result file per individual timepoint
#    # that it checks
#    results_bucket="gs://calitp-py-ci/gtfs-rt-validator-api/test_output_full",
#
#    verbose=True
#)
