from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
import uuid

from pathlib import Path

pb_files = list(Path("data/bucket").glob("*/*/*/*.pb"))

entities = []
headers = []

for fname in pb_files:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(open(fname, "rb").read())

    d = json_format.MessageToDict(feed)

    header = d.get("header", {})
    header["header_key"] = str(uuid.uuid4())

    for entity in d.get("entity", []):
        # just stash header on entity dict
        entity["header_key"] = header["header_key"]
        entities.append(entity)

    headers.append(header)

import pandas as pd
df_entities = pd.json_normalize(entities)
df_headers = pd.json_normalize(headers)

df_headers.merge(df_entities, on = "header_key")
