import os
import subprocess

from update_utils.update_markets import update_markets
from update_utils.update_goldsky import update_goldsky
from update_utils.process_live import process_live

SNAPSHOT_URL = "https://polydata-archive.s3.us-east-1.amazonaws.com/orderFilled_complete.csv.xz"

def download_snapshot():
    if not os.path.isfile("goldsky/orderFilled.csv"):
        print("Downloading data snapshot...")
        os.makedirs("goldsky", exist_ok=True)
        subprocess.run(["curl", "-L", SNAPSHOT_URL, "-o", "goldsky/orderFilled.csv.xz"], check=True)
        subprocess.run(["xz", "-d", "goldsky/orderFilled.csv.xz"], check=True)
        print("Snapshot ready!")
    else:
        print("Snapshot already exists, skipping download")

if __name__ == "__main__":
    download_snapshot()
    print("Updating markets")
    update_markets()
    print("Updating goldsky")
    update_goldsky()
    print("Processing live")
    process_live()
