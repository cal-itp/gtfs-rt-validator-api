from calitp.storage import get_fs
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# THE 5th number (e.g. last 7) roughly changes the day
START_TIMESTAMP = 1627700000

KEEP_DT = datetime(2021, 8, 1)

fs = get_fs()

#fs.glob("gs://gtfs-data/rt-fixed-timestamp/16277*")
all_fnames = fs.glob("gs://gtfs-data/rt/1*")


def move(entry):
    print("moving: %s" %str(entry))
    fs.mv(entry[0], entry[1], recursive=True)


to_move = []
n_moved = 0

for fname in all_fnames:
    timestamp = fname.split("/")[-1]

    # don't try to move a date
    if timestamp.startswith("202"):
        continue

    dt = datetime.fromtimestamp(int(timestamp))

    fname_parent = "/".join(fname.split("/")[:-1])
    new_name = f'{fname_parent}/{dt.isoformat()}'

    print("MOVING")
    print(fname)
    print(new_name)

    to_move.append((fname, new_name))
    #fs.mv(fname, new_name, recursive=True)

    n_moved += 1
    print("N Moved: %s" %n_moved)



pool = ThreadPoolExecutor(8)
list(pool.map(move, to_move))


