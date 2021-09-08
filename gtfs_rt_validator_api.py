__version__ = "0.0.1"

import os
import json
import subprocess
import warnings

import argh
from argh import arg

from tempfile import TemporaryDirectory
from pathlib import Path

try:
    JAR_PATH = os.environ.get("GTFS_VALIDATOR_JAR")
except KeyError:
    raise Exception("Must set the environment variable GTFS_VALIDATOR_JAR")


def validate(gtfs_file, rt_path, out_file=None, verbose=False):

    if not isinstance(gtfs_file, str):
        raise NotImplementedError("gtfs_file must be a string")

    stderr = subprocess.DEVNULL if not verbose else None
    stdout = subprocess.DEVNULL if not verbose else None

    with TemporaryDirectory() as tmp_out_dir:
        subprocess.check_call([
            "java",
            "-jar", JAR_PATH,
            "-gtfs", gtfs_file,
            "-gtfsRealtimePath", gtfs_file,
            ], stderr=stderr, stdout=stdout)


def validate_gcs_bucket(
        project_id, token, gtfs_schedule_path, gtfs_rt_path,
        gtfs_rt_glob="2021-08-01",
        out_file=None, verbose=False
        ):

    import gcsfs
    import shutil

    fs = gcsfs.GCSFileSystem(project_id, token=token)

    with TemporaryDirectory() as tmp_dir:
        path_gtfs = f"{tmp_dir}/gtfs"
        path_gtfs_zip = f"{path_gtfs}.zip"
        path_rt = f"{tmp_dir}/rt"

        # fetch and zip gtfs schedule
        fs.get(gtfs_schedule_path, path_gtfs, recursive=True)
        shutil.make_archive(path_gtfs, "zip", path_gtfs)

        # fetch rt data
        # TODO: replace with gtfs_rt_glob
        if gtfs_rt_date:
            fs.glob(f"{gtfs_rt_date}T*/")


def download_rt_files(date="2021-08-01"):



