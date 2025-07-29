import asyncio
import httpx
import os
import json
import random
import logging
import uuid
from typing import List, Optional, Dict, Tuple
from datetime import datetime, timezone
from typing import List, Optional, Dict, Tuple
from aiolimiter import AsyncLimiter
from app.model.models import ErrorLog
from app.utils.CircuitBreaker import CircuitBreaker


# Rate limiters
endpoint_limiters: Dict[str, AsyncLimiter] = {}

# retry and limit function
async def fetch_with_retry_and_limit(client, url: str, headers: Optional[Dict[str, str]], 
                                     limiter: AsyncLimiter, retries: int = 3, circuit_breaker: CircuitBreaker = None):
    for attempt in range(retries):
        try:
            async with limiter:
                resp = await circuit_breaker.call(lambda: client.get(url, headers=headers))
                resp.raise_for_status()
                return resp
        except Exception as e:
            if attempt == retries - 1:
                raise
            backoff = 2 ** attempt + random.uniform(0, 0.5)
            await asyncio.sleep(backoff)

# Fetch data from paginated API and save to local files
async def fetch_paginated_to_files(endpoint_template: str, endpoint_name: str, paged: bool = False, 
                                   headers: Optional[Dict[str, str]] = None) -> Tuple[List[ErrorLog], int]:
    errors = []
    page = 1
    skip = 0
    if endpoint_name not in endpoint_limiters:
        # at most 5 requests per second
        endpoint_limiters[endpoint_name] = AsyncLimiter(5, 1)
    limiter = endpoint_limiters[endpoint_name]
    os.makedirs("temp_data", exist_ok=True)

    # this is for counting the number of requests
    total_items = 0

    circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=10)
    logging.info(f"Starting to fetch data from {endpoint_name}")

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            while True:
                url = endpoint_template.format(page=page, skip=skip)
                logging.debug(f"Fetching data from {url}")
                try:
                    resp = await fetch_with_retry_and_limit(client, url, headers, limiter, circuit_breaker=circuit_breaker)
                    json_data = resp.json()
                    # logging.debug(f"Received response from {url} with json_data {json_data}")
                    if isinstance(json_data, list):
                        items = json_data
                    else:
                        items = json_data.get(
                            "data") or json_data.get("products") or []

                    total_items += 1
                    if not isinstance(items, list) or not items:
                        break

                    with open(f"temp_data/{endpoint_name}_{page}_{uuid.uuid4()}.json", "w", encoding="utf-8") as f:
                        json.dump(items, f)

                    if not paged:
                        break
                    page += 1
                    skip += 20
                except Exception as e:
                    error_message = f"Error fetching data from {url}: {e}"
                    logging.error(error_message)
                    errors.append(ErrorLog(endpoint=url, error=error_message, timestamp=datetime.now(
                        timezone.utc).isoformat() + "Z"))
                    break
        except Exception as e:
            errors.append(ErrorLog(endpoint=endpoint_name, error=f"General: {e}", timestamp=datetime.now(
                timezone.utc).isoformat() + "Z"))
    return errors, total_items