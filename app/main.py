import asyncio
import argparse
import httpx
import os
import json
import random
import logging
import sys
import uuid
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple
from collections import defaultdict
from fastapi import FastAPI
from app.model.models import ErrorLog, Summary, PriceMetrics, Metrics, OutputModel
from app.utils.Processor import process_files_to_products
from app.utils.UrlFetcher import fetch_paginated_to_files


# Note: for usage, please go to the end of the file, there is a CLI interface implemented
# two options to run the project:
# 1. run the FastAPI server and use `http://127.0.0.1:8000/docs` access the /aggregate endpoint
# 2. run the script with command line arguments to fetch data from custom endpoints

# For detailed debug information, use DEBUG level, for general information, use INFO level
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")

# ---------- FastAPI ----------
# example usage (in the Concurrent-data-processing-pipeline folder):
# uvicorn app.main:app --reload
# then access http://127.0.0.1:8000/docs
app = FastAPI()

@app.get("/aggregate", response_model=OutputModel)
async def aggregate():
    known_sources = ["jsonplaceholder", "reqres", "dummyjson", "httpbin"]
    start = datetime.now(timezone.utc).timestamp()
    all_errors: List[ErrorLog] = []
    source_counts = {}

    logging.info("Started aggregate process.")

    fetch_tasks = [
        fetch_paginated_to_files("https://jsonplaceholder.typicode.com/posts", "jsonplaceholder", paged=False),
        fetch_paginated_to_files("https://reqres.in/api/users?page={page}", "reqres", paged=True, headers={"x-api-key": "reqres-free-v1"}),
        fetch_paginated_to_files("https://dummyjson.com/products?limit=20&skip={skip}", "dummyjson", paged=True),
        fetch_paginated_to_files("https://httpbin.org/delay/1", "httpbin", paged=False)
    ]
    logging.info("Fetching data from sources...")
    results = await asyncio.gather(*fetch_tasks)
    
    for (err, count), name in zip(results, known_sources):
        all_errors.extend(err)
        source_counts[name] = count
        logging.debug(f"Source: {name}, Errors: {len(err)}, Count: {count}") 

    logging.info(f"Fetched data from all sources, processing files...")
    products = process_files_to_products()
    logging.info(f"Processed {len(products)} products.")

    duration = round(datetime.now(timezone.utc).timestamp() - start, 2)
    success_rate = round(len(products) / (len(all_errors) + len(products)), 2) if products else 0

    # price_metrics
    valid_prices = [p.price for p in products if p.price is not None]
    if valid_prices:
        price_metrics = PriceMetrics(
            min_price=round(min(valid_prices), 2),
            max_price=round(max(valid_prices), 2),
            average_price=round(sum(valid_prices) / len(valid_prices), 2),
            valid_price_count=len(valid_prices)
        )
    else:
        price_metrics = PriceMetrics()
        logging.debug("No valid prices found, defaulting to empty metrics.")
    
    # category_distribution
    category_distribution = defaultdict(int)
    for p in products:
        category_distribution[p.category or "unknown"] += 1

    # source_request_counts
    source_request_counts = {src: source_counts.get(src, 0) for src in known_sources}

    summary = Summary(
        total_products=len(products),
        processing_time_seconds=duration,
        success_rate=success_rate,
        sources=list(set(p.source for p in products))
    )
    logging.info("Returning aggregated response.")

    return OutputModel(
        summary=summary,
        products=products,
        errors=all_errors,
        metrics=Metrics(
            price_metrics=price_metrics,
            category_distribution=dict(category_distribution),
            source_request_counts=source_request_counts
        )
    )

# ---------- CLI aggregration ----------
# please go to if __name__ == "__main__" to see how to run this function
# example usages are provided above if __name__ == "__main__"
async def run_cli_aggregation(custom_endpoints: List[Tuple[str, str, bool, Optional[Dict[str, str]]]], max_workers: int):
    start = datetime.now(timezone.utc).timestamp()
    all_errors: List[ErrorLog] = []
    source_counts = {}

    fetch_tasks = [
        fetch_paginated_to_files(url, name, paged=paged, headers=headers)
        for url, name, paged, headers in custom_endpoints
    ]

    results = await asyncio.gather(*fetch_tasks)
    for (err, count), (_, name, _, _) in zip(results, custom_endpoints):
        all_errors.extend(err)
        source_counts[name] = count

    products = process_files_to_products(max_workers)
    duration = round(datetime.now(timezone.utc).timestamp() - start, 2)
    success_rate = round(len(products) / (len(all_errors) +
                         len(products)), 2) if products else 0

    valid_prices = [p.price for p in products if p.price is not None]
    if valid_prices:
        price_metrics = PriceMetrics(
            min_price=round(min(valid_prices), 2),
            max_price=round(max(valid_prices), 2),
            average_price=round(sum(valid_prices) / len(valid_prices), 2),
            valid_price_count=len(valid_prices)
        )
    else:
        price_metrics = PriceMetrics()

    category_distribution = defaultdict(int)
    for p in products:
        category_distribution[p.category or "unknown"] += 1

    source_request_counts = {src: source_counts.get(
        src, 0) for _, src, _, _ in custom_endpoints}
    summary = Summary(
        total_products=len(products),
        processing_time_seconds=duration,
        success_rate=success_rate,
        sources=list(set(p.source for p in products))
    )

    output = OutputModel(
        summary=summary,
        products=products,
        errors=all_errors,
        metrics=Metrics(
            price_metrics=price_metrics,
            category_distribution=dict(category_distribution),
            source_request_counts=source_request_counts
        )
    )

    # Save output to a file
    output_dir = './output'
    os.makedirs(output_dir, exist_ok=True)

    date_str = datetime.now().strftime('%Y-%m-%d-%H-%M')
    file_name = f"{date_str}_output.json"
    file_path = os.path.join(output_dir, file_name)

    with open(file_path, 'w') as f:
        json.dump(output.model_dump(), f, indent=2)

    logging.info(f"Output saved to {file_path}")
    # print(json.dumps(output.model_dump(), indent=2))


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Run aggregation from command line")
    parser.add_argument(
        '--url', nargs='+', help='List of URLs to fetch (format: url|name|paged[0/1])', required=False)
    parser.add_argument(
        '--args_file', help='Path to a JSON file with URLs and headers configuration', required=False)
    parser.add_argument('--workers', type=int, default=8,
                        help='Number of concurrent workers (default is 8)')
    return parser.parse_args()


def read_args_from_file(file_path: str):
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON format in file: {e}")
        sys.exit(1)


# Example usage:
# python3 main.py --url "https://dummyjson.com/products?limit=20&skip={skip}|dummyjson|1"
# python3 main.py --url "https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0" --workers 7
# python3 main.py --url "https://dummyjson.com/products?limit=20&skip={skip}|dummyjson|1"
#                       "https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0"
# python3 main.py --url "https://jsonplaceholder.typicode.com/posts|jsonplaceholder|0" --args_file "args.json"
if __name__ == "__main__":
    args = parse_cli_args()

    endpoints = []

    if args.url:
        for entry in args.url:
            try:
                logging.info(f"Processing URL entry: {entry}")
                url, name, paged = entry.split("|")
                paged = bool(int(paged))
                endpoints.append((url, name, paged, None))
            except:
                logging.error(f"Invalid input format for --url: {entry}")
                sys.exit(1)

    if args.args_file:
        file_endpoints = read_args_from_file(args.args_file)
        logging.info(f"Processing endpoints from file: {args.args_file}")
        for entry in file_endpoints:
            url = entry.get("url")
            name = entry.get("name")
            paged = entry.get("paged", False)
            headers = entry.get("headers", None)
            if headers:
                # change headers to immutable type
                headers = frozenset(headers.items())
            endpoints.append((url, name, paged, headers))

    # deduplicate endpoints
    endpoints = list(set(endpoints))

    if endpoints:
        asyncio.run(run_cli_aggregation(endpoints, args.workers))
    else:
        logging.error("No valid URLs or file configuration found.")
        sys.exit(1)
