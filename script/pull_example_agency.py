import yaml
import requests
from time import sleep
from datetime import datetime

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor


FEED_TYPES = ["vehicle_positions", "service_alerts", "trip_updates"]


def download_rt_feed_url(url, out_name=None):
    r = requests.get(url, verify=False)

    if out_name:
        Path(out_name).parent.mkdir(parents = True, exist_ok = True)
        with open(out_name, "wb") as f:
            f.write(r.content)

    return r


def download_rt_feed(feed, ii=None, save_as=None):
    for feed_type in FEED_TYPES:
        out_name = save_as.format(feed_type=feed_type, ii=ii)
        feed_key = f"gtfs_rt_{feed_type}_url"
        feed_url = feed[feed_key]

        download_rt_feed_url(feed_url, out_name)


# main script -----

agencies = yaml.load(open("data/agencies.yml")) 
feed = agencies["commerce-municipal-bus-lines"]["feeds"][0]

for ii in range(3):
    download_rt_feed(feed, int(datetime.now().timestamp()), "data/commerce/rt/{feed_type}_{ii}.pb")
    sleep(20)
