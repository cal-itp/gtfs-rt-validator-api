# GTFS RT Validator API

This library wraps [CUTR-at-USF/gtfs-realtime-validator](https://github.com/CUTR-at-USF/gtfs-realtime-validator),
to provide the following in python:

* a `validate` function for running validation.
* a commandline tool for fetching and validating cal-itp realtime.

It relies on setting the `GTFS_VALIDATOR_JAR` environment variable with the path of the RT validator.
A Dockerfile is provided for ease of use.

## Install


## Testing

```shell
docker-compose run gtfs-rt-validator pytest tests
```

## Running validator

### Setup

To execute using docker, first run..

```shell
# start docker
docker-compose run gtfs-rt-validator /bin/bash

# from docker, open python repl
python3
```

### Fetching GTFS RT data

```python
from gtfs_rt_validator_api import download_gtfs_schedule_zip, download_rt_files
from calitp.storage import get_fs

fs = get_fs()

# creates a file named gtfs_schedule.zip with some zipped GTFS data
download_gtfs_schedule_zip(
    "gs://gtfs-data/schedule/2021-09-01T00:00:00+00:00/106_0", "gtfs_schedule"
)

# downloads an individual feed's RT files into example_gtfs_rt
download_rt_files("example_gtfs_rt", fs, glob_path="gs://gtfs-data/rt/2021-09-01T*/106/0/*")

# downloads RT for all feeds into exaple_gtfs_rt_many
# each feed is a subdirectory of form {calitp_itp_id}/{calitp_url_number}/
# e.g. example_gtfs_rt_many/106/0/<some rt file>
download_rt_files("example_gtfs_rt_many", fs, date="2021-09-01")
```

### Validating a folder

TODO

### Validating a GCS bucket

Then, from python you can run the following to validate a GCS bucket

```python
from gtfs_rt_validator_api import validate_gcs_bucket

validate_gcs_bucket(
    "cal-itp-data-infra",
    None,
    "gs://gtfs-data/schedule/2021-09-01T00:00:00+00:00/106_0",
    gtfs_rt_glob_path="gs://gtfs-data/rt/2021-09-01T*/106/0/*",
    out_dir="test_out_106",

    # uncomment to push the resulting validation files up to a gcs bucket.
    # note that the validator produces 1 result file per individual timepoint
    # that it checks
    # results_bucket="gs://gtfs-data-test/rt-processed/validation",

    verbose=True
)
```

## Running the original validator

You can run the underlying [CUTR-at-USF/gtfs-realtime-validator](https://github.com/CUTR-at-USF/gtfs-realtime-validator) with the following code.

```
docker-compose run gtfs-rt-validator java \
    -jar /gtfs-realtime-validator-lib-1.0.0-SNAPSHOT.jar \
    -gtfs /application/data/commerce/gtfs.zip \
    -gtfsRealtimePath /application/data/commerce/rt
```
