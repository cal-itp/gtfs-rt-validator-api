# this script is used to push test data to a cloud bucket, to be used for tests.
import pandas as pd
from calitp.storage import get_fs

fs = get_fs()

# copy in realtime data
for ii in ["2021-10-01T00:00:11", "2021-10-01T00:00:31", "2021-10-01T00:00:51"]:
    fs.copy(
        f"gs://gtfs-data/rt/{ii}/126/0",
        f"gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/{ii}/126/0",
        recursive=True,
    )

# copy in schedule data
fs.copy(
    "gs://gtfs-data/schedule/2021-10-01T00:00:00+00:00/126_0",
    "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
    recursive=True,
)

# create a 1 row parameter file for validate_gcs_bucket_many ----
params = pd.DataFrame(
    {
        "gtfs_schedule_path": [
            "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126"
        ],
        "gtfs_rt_glob_path": [
            "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/2021*/126/0/*"
        ],
    }
)

# save to gcs
fs.pipe(
    "gs://calitp-py-ci/gtfs-rt-validator-api/validation_params.csv",
    params.to_csv(index=False).encode(),
)


# create a 25 row parameter file, pointing to production data ----
feeds = fs.ls("gtfs-data/rt/2021-10-01T00:00:11/")
itp_ids = [x.split("/")[-1] for x in feeds]

gtfs_schedules = [
    f"gs://gtfs-data/schedule/2021-10-01T00:00:00+00:00/{id}_0" for id in itp_ids
]
gtfs_rt = [f"gs://gtfs-data/rt/2021-10-01T00:00:*/{id}/0/*" for id in itp_ids]

params_many = pd.DataFrame(
    {"gtfs_schedule_path": gtfs_schedules, "gtfs_rt_glob_path": gtfs_rt}
)

# save to gcs
fs.pipe(
    "gs://calitp-py-ci/gtfs-rt-validator-api/validation_params_many.csv",
    params_many.to_csv(index=False).encode(),
)
