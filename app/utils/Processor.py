import os
import json
import logging
from typing import List
from datetime import datetime, timezone
from tqdm import tqdm
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from app.model.models import ProductItem

# Process a single file and extract product items
def process_file(filepath: str, products: List[ProductItem], lock: Lock, file_index: int, total_files: int, pbar: tqdm):
    logging.debug(
        f"Started processing file {file_index + 1}/{total_files}: {filepath}")
    with open(filepath, "r") as f:
        raw_items = json.load(f)
        local_products = []
        source = os.path.basename(filepath).split("_")[0]
        for data in raw_items:
            local_products.append(ProductItem(
                id=str(data.get("id")),
                title=data.get("title") or data.get(
                    "name") or f"{data.get('first_name', '')} {data.get('last_name', '')}",
                source=source,
                price=data.get("price"),
                category=data.get("category", "unknown"),
                processed_at=datetime.now(timezone.utc).isoformat() + "Z"
            ))
    with lock:
        products.extend(local_products)
    logging.debug(
        f"Successfully added {len(local_products)} products to the list.")
    os.remove(filepath)
    logging.debug(f"File {filepath} has been removed after processing.")

    # update progress bar
    pbar.update(1)
    pbar.set_postfix(
        {"Progress": f"{(file_index + 1) / total_files * 100:.1f}% finished"})


def process_files_to_products(max_workers=8) -> List[ProductItem]:
    products = []
    lock = Lock()
    filepaths = [os.path.join("temp_data", f) for f in os.listdir("temp_data")]

    total_files = len(filepaths)
    logging.info(f"Total files to process: {total_files}")

    # use ThreadPoolExecutor to process files concurrently
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # for better user experience, use tqdm to show progress
        with tqdm(total=total_files, desc="Processing files", unit="file", ncols=100) as pbar:
            for file_index, filepath in enumerate(filepaths):
                executor.submit(partial(process_file, filepath=filepath, products=products, lock=lock,
                                        file_index=file_index, total_files=total_files, pbar=pbar))

    return products