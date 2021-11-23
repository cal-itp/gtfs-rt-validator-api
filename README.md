# GTFS RT Validator API

This library wraps [CUTR-at-USF/gtfs-realtime-validator](https://github.com/CUTR-at-USF/gtfs-realtime-validator),
to provide the following in python:

* a `validate` function for running validation.
* a commandline tool for fetching and validating cal-itp realtime.

It relies on setting the `GTFS_VALIDATOR_JAR` environment variable with the path of the RT validator.
A Dockerfile is provided for ease of use.

## Install

From the root of this repo, run the following:

```
pip install -e .

export GTFS_VALIDATOR_JAR=<path to your GTFS RT validator JAR file>
```

This will let you import `gtfs_rt_validator_api`, or run the `gtfs-rt-validator` CLI.

Alternatively, you can use the Dockerfile in this repo (shown below).

## Deploy

1. Increment the version number in `gtfs_rt_validator_api.py`
2. Create a github release, where the tag is the version number.

This will run a github action that builds and pushes the docker image to the
[calitp container registry called gtfs-rt-validator-api](https://github.com/orgs/cal-itp/packages?repo_name=gtfs-rt-validator-api).



## Test

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

### Fetching and Validating GTFS RT data

```python
from gtfs_rt_validator_api import (
    download_gtfs_schedule_zip,
    download_rt_files,
    validate
)
from calitp.storage import get_fs

SCHEDULE_ZIP="gtfs_schedule_126"
RT_DIR="gtfs_rt_126"

fs = get_fs()

# creates a file named gtfs_schedule_106.zip with some zipped GTFS data
download_gtfs_schedule_zip(
    "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
    SCHEDULE_ZIP,
    fs
)

# downloads an individual feed's RT files into gtfs_rt_126
download_rt_files(
    RT_DIR,
    fs,
    glob_path="gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/*/126/0/*",
)

# validate data
# the results of validation are included in the RT_DIR as <filename>.results.json
validate(SCHEDULE_ZIP, RT_DIR)
```

### Validating a GCS bucket

Then, from python you can run the following to validate a GCS bucket

```python
from gtfs_rt_validator_api import validate_gcs_bucket

validate_gcs_bucket(
    "cal-itp-data-infra", 
    None,
    "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_schedule_126",
    "gs://calitp-py-ci/gtfs-rt-validator-api/gtfs_rt_126/2021*/126/0/*",

    # if out_dir is None, it uses a temporary directory
    out_dir="tests/data/validate_gcs_bucket",

    # optionally can save final results to a gcs bucket
    # results_bucket="gs://calitp-py-ci/gtfs-rt-validator-api/test_output"
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
