# this script is used to push test data to a cloud bucket, to be used for tests.
from calitp.storage import get_fs

fs = get_fs()

# copy in realtime data
for ii in ["2021-10-01T00:00:11", "2021-10-01T00:00:31", "2021-10-01T00:00:51"]:
    fs.copy(
        f"gs://gtfs-data/rt/{ii}/126/0",
        f"gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/{ii}/126/0",
        recursive=True
    )

# copy in schedule data
fs.copy(
        "gs://gtfs-data/schedule/2021-10-01T00:00:00+00:00/126_0",
    "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
    recursive=True
)
