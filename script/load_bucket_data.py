import pandas as pd
import uuid


from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format

from pathlib import Path

pb_files = list(Path("data/bucket/126").glob("rt/*.pb"))

def load_to_data_frame(pb_files):

    entities = []
    headers = []

    for fname in pb_files:
        calitp_itp_id, calitp_url_number = str(fname).split("/")[-3:-1]

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(open(fname, "rb").read())

        d = json_format.MessageToDict(feed)

        header = d.get("header", {})
        header["header_key"] = str(uuid.uuid4())
        header["calitp_itp_id"] = calitp_itp_id
        header["calitp_url_number"] = calitp_url_number

        for entity in d.get("entity", []):
            # just stash header on entity dict
            entity["header_key"] = header["header_key"]
            entities.append(entity)

        headers.append(header)

    df_entities = pd.json_normalize(entities)
    df_headers = pd.json_normalize(headers)
    
    return df_entities, df_headers


if __name__ == "__main__":
    df_entities, df_headers = load_to_data_frame(pb_files)
    df_vehicle_positions = df_headers.merge(df_entities, on = "header_key")

    df_vehicle_positions.to_csv("data/bucket_vehicle_positions.csv", index=False)
