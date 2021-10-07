__version__ = "0.0.1"

import os
import json
import subprocess
import warnings

import argh
from argh import arg

from tempfile import TemporaryDirectory
from pathlib import Path
from collections import defaultdict
from concurrent import futures

try:
    JAR_PATH = os.environ.get("GTFS_VALIDATOR_JAR")
except KeyError:
    raise Exception("Must set the environment variable GTFS_VALIDATOR_JAR")


def gather_results(rt_path):
    # TODO: complete functionality to unpack results into a DataFrame
    # Path(rt_path).glob("*.results.json")
    raise NotImplementedError()


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
            "-gtfsRealtimePath", rt_path,
            ], stderr=stderr, stdout=stdout)


def validate_gcs_bucket(
        project_id, token, gtfs_schedule_path, gtfs_rt_glob_path,
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
        download_gtfs_schedule_zip(path_gtfs)

        # fetch rt data
        # TODO: remove hard-coding
        download_rt_files(path_rt, fs, glob_path = gtfs_rt_glob_path)


def download_gtfs_schedule_zip(path_gtfs):
    # fetch and zip gtfs schedule
    fs.get(gtfs_schedule_path, path_gtfs, recursive=True)
    shutil.make_archive(path_gtfs, "zip", path_gtfs)
    

def download_rt_files(dst_dir, fs=None, date="2021-08-01", glob_path=None):
    if fs is None:
        raise NotImplementedError("Must specify fs")

    # {date}T{timestamp}/{itp_id}/{url_number}
    all_files = fs.glob(glob_path) if glob_path else fs.glob(f"{date}*/*/*/*")

    to_copy = []
    out_feeds = defaultdict(lambda: [])
    for src_path in all_files:
        dt, itp_id, url_number, src_fname = src_path.split("/")[-4:]

        if glob_path:
            dst_parent = Path(dst_dir)
        else:
            # if we are downloading multiple feeds, make each feed a subdir
            dst_parent = Path(dst_dir) / itp_id / url_number

        dst_parent.mkdir(parents=True, exist_ok=True)

        # TODO: check that this works
        dst_name = str(dst_parent / f"{src_fname}_{dt}.pb")

        to_copy.append([src_path, dst_name])
        out_feeds[(itp_id, url_number)].append(dst_name)

    print(f"Copyting {len(to_copy)} files")

    with futures.ThreadPoolExecutor(max_workers=30) as pool:
        list(pool.map(lambda args: fs.get(*args), to_copy))

    return out_feeds


def main():
    # TODO: make into simple CLI
    result = argh.dispatch_commands([
        validate, validate_gcs_bucket
        ])

    if result is not None:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
