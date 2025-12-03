import os
import gzip
import shutil
import logging
import requests
from pathlib import Path
from typing import List

# 1. Configuration & Logging Setup
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("IMDb Data Downloader")

BASE_URL = "https://datasets.imdbws.com/"
# Using Path objects is safer and cross-platform compatible
OUTPUT_DIR = Path("data/raw/imdb_data/") 

FILES_TO_DOWNLOAD = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz",
    "title.crew.tsv.gz",
]

def download_file(url: str, dest_path: Path) -> None:
    """Downloads a file in chunks to manage memory usage efficiently."""
    logger.info(f"Starting download: {url}")
    
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status() # Raises error for 4xx/5xx responses
            with open(dest_path, 'wb') as f:
                # 8kb chunks prevent loading huge files entirely into RAM
                for chunk in response.iter_content(chunk_size=8192): 
                    f.write(chunk)
        logger.info(f"Download complete: {dest_path.name}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading {url}: {e}")
        raise

def extract_gzip(file_path: Path) -> None:
    """Extracts a .gz file and removes the archive to save space."""
    output_path = file_path.with_suffix('') # Removes .gz
    logger.info(f"Extracting: {file_path.name} -> {output_path.name}")
    
    try:
        with gzip.open(file_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        # Cleanup: Remove the .gz file after extraction
        os.remove(file_path)
        logger.info(f"Extraction complete and archive removed: {file_path.name}")

    except Exception as e:
        logger.error(f"Failed to extract {file_path}: {e}")
        raise

def run_pipeline() -> None:
    """Orchestrates the download and extraction process."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    for filename in FILES_TO_DOWNLOAD:
        file_url = BASE_URL + filename
        gz_path = OUTPUT_DIR / filename
        tsv_path = OUTPUT_DIR / filename.replace('.gz', '')

        # Idempotency Check: Don't download if the unzipped file already exists
        if tsv_path.exists():
            logger.info(f"Skipping {filename}, extracted file already exists.")
            continue

        try:
            download_file(file_url, gz_path)
            extract_gzip(gz_path)
        except Exception:
            logger.error(f"Pipeline failed for {filename}. Moving to next.")
            continue

if __name__ == "__main__":
    run_pipeline()