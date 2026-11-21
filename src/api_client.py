"""Collects Formula 1 race results from the Jolpica F1 API and preserves the raw data."""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import pandas as pd
import requests

from src import config

log = logging.getLogger("api_client")

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class ApiError(RuntimeError):
    """The API could not return a usable response."""


def parse_payload(response: requests.Response) -> dict:
    """Decode the JSON body and check the MRData -> RaceTable -> Races structure."""
    try:
        payload = response.json()
        races = payload["MRData"]["RaceTable"]["Races"]
        int(payload["MRData"]["total"])
    except (ValueError, KeyError, TypeError) as exc:
        raise ApiError(f"Unexpected response format from {response.url}: {exc!r}") from exc
    if not isinstance(races, list):
        raise ApiError(f"'Races' is not a list in the response from {response.url}")
    return payload


def fetch_page(session: requests.Session, season: int, offset: int) -> dict:
    """GET one page of /{season}/results/, retrying network errors, 429 and 5xx with exponential backoff."""
    url = f"{config.API_BASE_URL}/{season}/results/"
    params = {"limit": config.API_PAGE_LIMIT, "offset": offset}

    for attempt in range(1, config.API_MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=config.API_TIMEOUT_S)
        except requests.RequestException as exc:
            problem = f"network error: {exc}"
        else:
            if response.status_code == 200:
                return parse_payload(response)
            if response.status_code not in RETRYABLE_STATUS_CODES:
                raise ApiError(f"{response.url} returned HTTP {response.status_code} (not retryable)")
            problem = f"HTTP {response.status_code}"

        wait_s = 2**attempt
        log.warning("Season %s offset %s: %s (attempt %d/%d), retrying in %ds",
                    season, offset, problem, attempt, config.API_MAX_RETRIES, wait_s)
        time.sleep(wait_s)

    raise ApiError(f"Season {season} offset {offset}: no successful response after {config.API_MAX_RETRIES} attempts")


def fetch_season(session: requests.Session, season: int) -> list[dict]:
    """Download every results page of a season (the API pages by result rows, max 100 per page)."""
    pages, offset, total = [], 0, None
    while total is None or offset < total:
        payload = fetch_page(session, season, offset)
        total = int(payload["MRData"]["total"])
        pages.append(payload)
        offset += config.API_PAGE_LIMIT
        time.sleep(config.API_REQUEST_DELAY_S)
    log.info("Season %d: %d results in %d pages", season, total, len(pages))
    return pages


def flatten_page(payload: dict, collected_at: str) -> list[dict]:
    """Turn the nested Race -> Results JSON into one flat record per driver per race; values stay as delivered."""
    records = []
    for race in payload["MRData"]["RaceTable"]["Races"]:
        circuit = race["Circuit"]
        for result in race["Results"]:
            driver = result["Driver"]
            constructor = result["Constructor"]
            fastest_lap = result.get("FastestLap", {})
            records.append({
                "record_id": f"{race['season']}_{race['round'].zfill(2)}_{driver['driverId']}",
                "season": race["season"],
                "round": race["round"],
                "race_name": race["raceName"],
                "race_date": race["date"],
                "race_start_time": race.get("time"),
                "circuit_id": circuit["circuitId"],
                "circuit_name": circuit["circuitName"],
                "country": circuit["Location"]["country"],
                "locality": circuit["Location"]["locality"],
                "driver_id": driver["driverId"],
                "driver_code": driver.get("code"),
                "driver_number": driver.get("permanentNumber"),
                "given_name": driver.get("givenName"),
                "family_name": driver.get("familyName"),
                "driver_dob": driver.get("dateOfBirth"),
                "driver_nationality": driver.get("nationality"),
                "constructor_id": constructor.get("constructorId"),
                "constructor_name": constructor.get("name"),
                "constructor_nationality": constructor.get("nationality"),
                "grid": result.get("grid"),
                "position": result.get("position"),
                "position_text": result.get("positionText"),
                "points": result.get("points"),
                "laps": result.get("laps"),
                "status": result.get("status"),
                "race_time_ms": result.get("Time", {}).get("millis"),
                "fastest_lap_rank": fastest_lap.get("rank"),
                "fastest_lap_number": fastest_lap.get("lap"),
                "fastest_lap_time": fastest_lap.get("Time", {}).get("time"),
                "collected_at": collected_at,
            })
    return records


def collect(seasons: list[int]) -> pd.DataFrame:
    """Collect all seasons, save the untouched API responses (JSON) and the flattened records (CSV)."""
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    raw_pages, records = [], []

    with requests.Session() as session:
        session.headers["User-Agent"] = "kbtu-dcp-sis2-f1-pipeline/1.0"
        for season in seasons:
            for payload in fetch_season(session, season):
                raw_pages.append(payload)
                records.extend(flatten_page(payload, collected_at))

    if not records:
        raise ApiError(f"The API returned no results for seasons {seasons}")

    config.DATA_DIR.mkdir(exist_ok=True)
    with open(config.RAW_JSON_PATH, "w", encoding="utf-8") as file:
        json.dump({"collected_at": collected_at, "seasons": seasons, "pages": raw_pages}, file, ensure_ascii=False)

    raw_df = pd.DataFrame(records)
    raw_df.to_csv(config.RAW_CSV_PATH, index=False)
    log.info("Collected %d records (%d columns) from %d API pages -> %s",
             len(raw_df), raw_df.shape[1], len(raw_pages), config.RAW_CSV_PATH.name)
    return raw_df


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seasons", type=int, nargs="+", default=config.SEASONS)
    args = parser.parse_args()
    config.setup_logging()
    try:
        collect(args.seasons)
    except ApiError as exc:
        log.error("Collection failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
