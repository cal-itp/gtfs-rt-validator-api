__version__ = "0.0.1"

import os
import json
import subprocess
import warnings
import shutil

import argh
from argh import arg

from tempfile import TemporaryDirectory
from pathlib import Path
from collections import defaultdict
from concurrent import futures

RT_BUCKET_FOLDER="gs://gtfs-data/rt"
RT_BUCKET_PROCESSED_FOLDER="gs://gtfs-data/rt-processed"
SCHEDULE_BUCKET_FOLDER="gs://gtfs-data/schedule"

# Note that the final {dt} is needed by the validator, which may read it as
# timestamp data
RT_FILENAME_TEMPLATE="{dt}__{itp_id}__{url_number}__{src_fname}__{dt}.pb"
N_THREAD_WORKERS = 30

try:
    JAR_PATH = os.environ["GTFS_VALIDATOR_JAR"]
except KeyError:
    raise Exception("Must set the environment variable GTFS_VALIDATOR_JAR")


# Utility funcs ----

def retry_on_fail(f, max_retries=2):
    n_retries = 0

    for n_retries in range(max_retries + 1):
        try:
            f()
        except Exception as e:
            if n_retries < max_retries:
                n_retries += 1
                warnings.warn("Function failed, starting retry: %s" % n_retries)
            else:
                raise e

def put_file(fs, src, dst):
    retry_on_fail(
        lambda: fs.put(src, dst),
        2
    )

def json_to_newline_delimited(in_file, out_file):
    data = json.load(open(in_file))
    with open(out_file, "w") as f:
        f.write("\n".join([json.dumps(record) for record in data]))

# Validation ==================================================================


def gather_results(rt_path):
    # TODO: complete functionality to unpack results into a DataFrame
    # Path(rt_path).glob("*.results.json")
    raise NotImplementedError()


def validate(gtfs_file, rt_path, out_file=None, verbose=False):

    if not isinstance(gtfs_file, str):
        raise NotImplementedError("gtfs_file must be a string")

    stderr = subprocess.DEVNULL if not verbose else None
    stdout = subprocess.DEVNULL if not verbose else None

    subprocess.check_call([
        "java",
        "-jar", JAR_PATH,
        "-gtfs", gtfs_file,
        "-gtfsRealtimePath", rt_path,
        "-sort", "name",
        ], stderr=stderr, stdout=stdout)


def validate_gcs_bucket(
        project_id, token, gtfs_schedule_path, gtfs_rt_glob_path=None,
        out_dir=None, results_bucket=None, verbose=False, 
        ):
    """
    Fetch and validate GTFS RT data held in a google cloud bucket.

    Parameters:
        project_id: name of google cloud project.
        token: token argument passed to gcsfs.GCSFileSystem.
        gtfs_schedule_path: path to a folder holding unpacked GTFS schedule data.
        gtfs_rt_glob_path: path that GCSFileSystem.glob can uses to list all RT files.
            Note that this is assumed to have the form {datetime}/{itp_id}/{url_number}/filename.
        out_dir: a directory to store fetched files and results in.
        results_dir: a bucket path to copy results to.
        verbose: whether to print helpful messages along the way.

    Note that if out_dir is unspecified, the validation occurs in a temporary directory.
        
    """

    import gcsfs

    fs = gcsfs.GCSFileSystem(project_id, token=token)

    if not out_dir:
        tmp_dir = TemporaryDirectory()
        tmp_dir_name = tmp_dir.name
    else:
        tmp_dir = None
        tmp_dir_name = out_dir

    if results_bucket and not results_bucket.endswith("/"):
        results_bucket = f"{results_bucket}/"

    final_json_dir = Path(tmp_dir_name) / "newline_json"

    try:
        print("Fetching data")

        dst_path_gtfs = f"{tmp_dir_name}/gtfs"
        dst_path_rt = f"{tmp_dir_name}/rt"

        # fetch and zip gtfs schedule
        download_gtfs_schedule_zip(gtfs_schedule_path, dst_path_gtfs, fs)

        # fetch rt data
        # TODO: remove hard-coding
        if gtfs_rt_glob_path is None:
            raise ValueError("One of gtfs rt glob path or date must be specified")


        download_rt_files(dst_path_rt, fs, glob_path = gtfs_rt_glob_path)

        print("Validating data")
        validate(f"{dst_path_gtfs}.zip", dst_path_rt, verbose=verbose)

        if results_bucket:
            # validator stores results as {filename}.results.json
            print(f"Putting data into results bucket: {results_bucket}")

            # fetch all results files created by the validator
            all_results = list(Path(dst_path_rt).glob("*.results.json"))

            
            final_json_dir.mkdir(exist_ok=True)
            final_files = []
            for result in all_results:
                # we appended a final timestamp to the files so that the validator
                # can use it to order them during validation. here, we remove that
                # timestamp, so we can use a single wildcard to select, eg..
                # *trip_updates.results.json
                result_out = "__".join(result.name.split("__")[:-1])

                json_to_newline_delimited(result, final_json_dir / result_out)
                final_files.append(final_json_dir / result_out)
            

            fs.put(final_files, results_bucket)

    except Exception as e:
        if isinstance(tmp_dir, TemporaryDirectory):
            tmp_dir.cleanup()

        raise e


def download_gtfs_schedule_zip(gtfs_schedule_path, dst_path, fs):
    # fetch and zip gtfs schedule
    fs.get(gtfs_schedule_path, dst_path, recursive=True)
    shutil.make_archive(dst_path, "zip", dst_path)
    

def download_rt_files(dst_dir, fs=None, date="2021-08-01", glob_path=None):
    """Download all files for an GTFS RT feed (or multiple feeds)

    If date is specified, downloads daily data for all feeds. Otherwise, if
    glob_path is specified, downloads data for a single feed.

    Parameters:
        date: date of desired feeds to download data from (e.g. 2021-09-01)
        glob_path: if specified, the path (including a wildcard) for downloading a 
                   single feed.

    """
    if fs is None:
        raise NotImplementedError("Must specify fs")

    # {date}T{timestamp}/{itp_id}/{url_number}
    all_files = fs.glob(glob_path) if glob_path else fs.glob(f"{RT_BUCKET_FOLDER}/{date}*/*/*/*")

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
        out_fname = RT_FILENAME_TEMPLATE.format(
            itp_id=itp_id,
            url_number=url_number,
            dt=dt,
            src_fname=src_fname
        )

        dst_name = str(dst_parent / out_fname)

        to_copy.append([src_path, dst_name])
        out_feeds[(itp_id, url_number)].append(dst_name)

    print(f"Copying {len(to_copy)} files")

    with futures.ThreadPoolExecutor(max_workers=N_THREAD_WORKERS) as pool:
        list(pool.map(lambda args: fs.get(*args), to_copy))


# Rectangling =================================================================


def main():
    # TODO: make into simple CLI
    result = argh.dispatch_commands([
        validate, validate_gcs_bucket
        ])

    if result is not None:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
