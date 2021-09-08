from calitp.storage import get_fs
from pathlib import Path

import sys
import pendulum

ITP_ID = sys.argv[1]

# uses the gcsfs package under the hood
fs = get_fs()

demo_files = {
        "vehicle_positions": fs.glob(f"gs://gtfs-data/rt/16257*/{ITP_ID}/0**gtfs_rt_vehicle_positions_url"),
        "trip_updates": fs.glob(f"gs://gtfs-data/rt/16257*/{ITP_ID}/0**gtfs_rt_trip_updates_url"),
        "service_alerts": fs.glob(f"gs://gtfs-data/rt/16257*/{ITP_ID}/0**gtfs_rt_service_alerts_url"),
        }

for dst_fname, src_files in demo_files.items():
    print(f"found {len(src_files)} files")
    for src_file in src_files:
        bucket, rt_dir, timestamp, itp_id, url_number, fname = src_file.split("/") 

        # src_file 
        # .replace("gtfs-data/rt/", "data/bucket/")
        # .replace("gtfs_rt_", "")
        # .replace("_url", "") + ".pb"
        #
        date_utc = str(pendulum.from_timestamp(int(timestamp)))
        dst_file = f"data/bucket/{ITP_ID}/rt/{dst_fname}__{date_utc}.pb"

        print(dst_file)
        Path(dst_file).parent.mkdir(parents = True, exist_ok = True)
        fs.get(f"gs://{src_file}", dst_file)


