from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
from pathlib import Path
import pandas as pd
import os


def parse_pb(path):
    """
    Convert pb file to Python dictionary
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(open(path, "rb").read())
    d = json_format.MessageToDict(feed)
    return d


def pull_value(x, path):
    """
    Safe extraction for pulling entity values
    """
    crnt_obj = x
    for attr in path.split("."):
        try:
            crnt_obj = crnt_obj[attr]
        except KeyError:
            return None
    return crnt_obj


def get_header_details(x):
    """
    Returns a dictionary of header values to be added to a dataframe
    """
    return {"header_timestamp": pull_value(x, "header.timestamp")}


def get_entity_details(x):
    """
    Returns a list of dicts containing entity values to be added to a dataframe
    """
    entity = x.get("entity")
    details = []
    if entity is not None:
        for e in entity:
            d = {
                "entity_id": pull_value(e, "id"),
                "vehicle_id": pull_value(e, "vehicle.vehicle.id"),
                "vehicle_trip_id": pull_value(e, "vehicle.trip.tripId"),
                "vehicle_timestamp": pull_value(e, "vehicle.timestamp"),
                "vehicle_position_latitude": pull_value(e, "vehicle.position.latitude"),
                "vehicle_position_longitude": pull_value(
                    e, "vehicle.position.longitude"
                ),
            }
            details.append(d)
    return details


def rectangle_positions(x):
    """
    Create a vehicle positions dataframe from parsed pb files
    """
    header_details = get_header_details(x)
    entity_details = get_entity_details(x)
    if len(entity_details) > 0:
        rectangle = pd.DataFrame(entity_details)
        for k, v in header_details.items():
            rectangle[k] = v
        return rectangle
    else:
        return None


# Files of interest
positions_pb = list(Path("data/bucket/126/rt").glob("vehicle_positions__*.pb"))

positions_parsed = [*map(parse_pb, positions_pb)]

positions_dfs = [*map(rectangle_positions, positions_parsed)]

positions_rectangle = pd.concat([df for df in positions_dfs if df is not None])

positions_rectangle.to_csv("output/positions_rectangle.csv", index=False)

# https://pandas.pydata.org/pandas-docs/version/1.1.5/reference/api/pandas.DataFrame.to_parquet.html
# Could be a benefit to use partition_cols
positions_rectangle.to_parquet("output/positions_rectangle.parquet", index=False)

# Raw Size in MB
raw_file_size = round(sum([*map(os.path.getsize, positions_pb)]) / (1024 * 1024), 2)
print("The raw vehicle positions pb files are {size} MB".format(size=raw_file_size))


print("")
csv_file_size = round(
    os.path.getsize("output/positions_rectangle.csv") / (1024 * 1024), 2
)
print("The rectangle'd vehicle positions csv is {size} MB".format(size=csv_file_size))

print("")
parquet_file_size = round(
    os.path.getsize("output/positions_rectangle.parquet") / (1024 * 1024), 2
)
print(
    "The rectangle'd vehicle positions parquet is {size} MB".format(
        size=parquet_file_size
    )
)

# QUESTION: How will we get file name?
