import pandas as pd

from calitp.tables import tbl
from siuba import *

from datetime import datetime

# this is when the first RT file is from
rt_extract_date = str(datetime.fromtimestamp(1625763502).date())

filter_for_date = filter(
    _.calitp_extracted_at < rt_extract_date,
    _.calitp_deleted_at.fillna("2099-01-01") > rt_extract_date
)

tbl_trips = tbl.gtfs_schedule_type2.trips() >> filter_for_date

df_trips = tbl_trips >> collect()

df_vehicle_positions = pd.read_csv("data/bucket_vehicle_positions.csv")

# combine vehicle_positions and trips
# potential questions: which vehicle trips don't have entries in the trips table?
# e.g. in this case, trip specific columns would be NULL in the joined data
df_joined = left_join(
    # may want to select a few specific columns
    df_vehicle_positions >> rename(trip_id = "vehicle.trip.tripId"),
    df_trips,
    ["calitp_itp_id", "calitp_url_number", "trip_id"]
)
