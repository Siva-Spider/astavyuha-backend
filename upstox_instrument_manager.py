import requests
import os
from datetime import date
import shutil
from pathlib import Path
import re  # Used for regex matching file names

# --- Configuration ---
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
DATA_DIR = Path("data")
DATE_PATTERN = r"instruments_\d{4}-\d{2}-\d{2}\.csv\.gz$"  # Regex to match dated files
LATEST_LINK_FILENAME = "latest_instruments.csv.gz"
TODAY_FILENAME = f"instruments_{date.today().isoformat()}.csv.gz"


def cleanup_old_files():
    """
    Deletes all dated instrument files in the data directory, except for today's file.
    """
    print("Starting cleanup of old dated files...")
    today_file_path = DATA_DIR / TODAY_FILENAME

    for item in DATA_DIR.iterdir():
        # Check if the file matches the dated naming convention and is NOT today's file
        if item.is_file() and re.match(DATE_PATTERN, item.name) and item != today_file_path:
            try:
                os.remove(item)
                print(f"  Deleted old file: {item.name}")
            except OSError as e:
                print(f"  Error deleting {item.name}: {e}")
    print("Cleanup complete.")


def update_instrument_file():
    """
    Downloads the latest instrument file only if it hasn't been done today,
    updates the 'latest' copy, and performs cleanup.
    """
    DATA_DIR.mkdir(exist_ok=True)
    today_file_path = DATA_DIR / TODAY_FILENAME
    latest_link_path = DATA_DIR / LATEST_LINK_FILENAME

    # 1. Check if today's file already exists
    if today_file_path.exists():
        print(f"File for {date.today()} already exists. Skipping download.")
        # Ensure the 'latest' copy points to today's file
        shutil.copy2(today_file_path, latest_link_path)
        cleanup_old_files()  # Run cleanup even if skipping download
        return

    # 2. Download the file
    print(f"Downloading new instrument file from Upstox...")
    try:
        response = requests.get(UPSTOX_URL, stream=True)
        response.raise_for_status()

        with open(today_file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"Successfully saved new file: {today_file_path}")

        # 3. Update the 'latest' file that the API will serve
        shutil.copy2(today_file_path, latest_link_path)
        print(f"Updated {LATEST_LINK_FILENAME} for API access.")

        # 4. Cleanup old files
        cleanup_old_files()

    except requests.exceptions.RequestException as e:
        print(f"Error during download: {e}")


if __name__ == '__main__':
    update_instrument_file()
