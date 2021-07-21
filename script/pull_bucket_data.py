from calitp.storage import get_fs
from pathlib import Path

# uses the gcsfs package under the hood
fs = get_fs()

demo_files = [
        *fs.glob("gs://gtfs-data/rt/1625763502/**_url"),
        *fs.glob("gs://gtfs-data/rt/1625763522/**_url"),
        *fs.glob("gs://gtfs-data/rt/1625763542/**_url"),
        *fs.glob("gs://gtfs-data/rt/1625763562/**_url")
        ]

for src_file in demo_files:
    dst_file = (
            src_file 
            .replace("gtfs-data/rt/", "data/bucket/")
            .replace("gtfs_rt_", "")
            .replace("_url", "") + ".pb"
            )

    Path(dst_file).parent.mkdir(parents = True, exist_ok = True)
    fs.get(f"gs://{src_file}", dst_file)


