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
    csv_path = os.path.join(DATA_DIR, "goldsky", "orderFilled.csv")
    if not os.path.isfile(csv_path):
        os.makedirs(os.path.join(DATA_DIR, "goldsky"), exist_ok=True)
        xz_path = csv_path + ".xz"

        print("Downloading snapshot...")
        with urllib.request.urlopen(SNAPSHOT_URL) as response, open(xz_path, "wb") as f:
            chunk_size = 8 * 1024 * 1024
            downloaded = 0
            while True:
                chunk = response.read(chunk_size)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                print(f"Downloaded {downloaded / 1024 / 1024:.0f} MB...")

        print("Extracting...")
        with lzma.open(xz_path) as f_in, open(csv_path, "wb") as f_out:
            chunk_size = 8 * 1024 * 1024
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)

        os.remove(xz_path)
        print("Snapshot ready!")
    else:
        print("Snapshot already exists, skipping download")

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.chdir("/app")
    download_snapshot()
    while True:
        print("Running pipeline...")
        update_markets()
        update_goldsky()
        process_live()
        print("Done. Sleeping 1 hour...")
        time.sleep(3600)
