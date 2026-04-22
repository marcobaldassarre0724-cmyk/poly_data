import os
import time
import urllib.request
import lzma

from update_utils.update_markets import update_markets
from update_utils.update_goldsky import update_goldsky
from update_utils.process_live import process_live

SNAPSHOT_URL = "https://polydata-archive.s3.us-east-1.amazonaws.com/orderFilled_complete.csv.xz"
DATA_DIR = "/app/data"

def download_snapshot():
    csv_path = os.path.join(DATA_DIR, "goldsky/orderFilled.csv")
    if not os.path.isfile(csv_path):
        print("Downloading data snapshot...")
        os.makedirs(os.path.join(DATA_DIR, "goldsky"), exist_ok=True)
        xz_path = csv_path + ".xz"
        urllib.request.urlretrieve(SNAPSHOT_URL, xz_path)
        print("Extracting...")
        with lzma.open(xz_path) as f_in, open(csv_path, "wb") as f_out:
            f_out.write(f_in.read())
        os.remove(xz_path)
        print("Snapshot ready!")
    else:
        print("Snapshot already exists, skipping download")

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.chdir(DATA_DIR)
    download_snapshot()
    while True:
        print("Running pipeline...")
        update_markets()
        update_goldsky()
        process_live()
        print("Done. Sleeping 1 hour...")
        time.sleep(3600)
