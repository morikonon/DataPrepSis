"""Collects Stack Overflow questions and the dates of their answers from the Stack Exchange API; keeps the raw data."""

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
NULLABLE_INT_FIELDS = ["accepted_answer_id", "last_edit_date", "closed_date", "owner_user_id", "owner_reputation",
                       "owner_accept_rate", "first_answer_date", "accepted_answer_date"]


class ApiError(RuntimeError):
    """The API could not return a usable response."""


def parse_payload(response: requests.Response) -> dict:
    """Decode the JSON body and check the common wrapper: {"items": [...], "has_more": ..., "quota_remaining": ...}."""
    try:
        payload = response.json()
        items, has_more = payload["items"], payload["has_more"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ApiError(f"Unexpected response format from {response.url}: {exc!r}") from exc
    if not isinstance(items, list) or not isinstance(has_more, bool):
        raise ApiError(f"'items' is not a list in the response from {response.url}")
    return payload


def error_text(response: requests.Response) -> str:
    """The API reports errors as {"error_id", "error_name", "error_message"}; fall back to the HTTP status."""
    try:
        body = response.json()
        return f"HTTP {response.status_code} {body['error_name']}: {body['error_message']}"
    except (ValueError, KeyError, TypeError):
        return f"HTTP {response.status_code}"


def fetch_page(session: requests.Session, path: str, params: dict) -> dict:
    """GET one page, retrying network errors, throttling and 5xx with exponential backoff."""
    url = f"{config.API_BASE_URL}/{path}"
    params = {"site": config.API_SITE, "pagesize": config.API_PAGE_SIZE, "filter": "default", **params}
    if config.API_KEY:
        params["key"] = config.API_KEY

    for attempt in range(1, config.API_MAX_RETRIES + 1):
        try:
            response = session.get(url, params=params, timeout=config.API_TIMEOUT_S)
        except requests.RequestException as exc:
            problem = f"network error: {exc}"
        else:
            if response.status_code == 200:
                payload = parse_payload(response)
                if "backoff" in payload:  # the API asks clients to pause before calling the same method again
                    log.warning("API requested a backoff of %ss", payload["backoff"])
                    time.sleep(payload["backoff"])
                return payload
            problem = error_text(response)
            if response.status_code not in RETRYABLE_STATUS_CODES and "throttle" not in problem:
                raise ApiError(f"/{path} failed with {problem} (not retryable)")

        wait_s = 2**attempt
        log.warning("/%s page %s: %s (attempt %d/%d), retrying in %ds",
                    path, params.get("page"), problem, attempt, config.API_MAX_RETRIES, wait_s)
        time.sleep(wait_s)

    raise ApiError(f"/{path} page {params.get('page')}: no successful response after {config.API_MAX_RETRIES} attempts")


def fetch_all_pages(session: requests.Session, path: str, params: dict) -> list[dict]:
    """Follow `has_more` page by page up to the configured page limit."""
    pages = []
    for page in range(1, config.API_MAX_PAGES_PER_TAG + 1):
        payload = fetch_page(session, path, {**params, "page": page})
        pages.append(payload)
        time.sleep(config.API_REQUEST_DELAY_S)
        if not payload["has_more"]:
            break
    else:
        log.warning("/%s: stopped at the limit of %d pages, more data is available", path, config.API_MAX_PAGES_PER_TAG)
    return pages


def fetch_questions(session: requests.Session, tag: str) -> list[dict]:
    """All questions with `tag` created inside the collection window, oldest first."""
    from_ts, to_ts = config.unix_range()
    pages = fetch_all_pages(session, "questions", {
        "tagged": tag, "fromdate": from_ts, "todate": to_ts, "sort": "creation", "order": "asc"})
    log.info("Tag %-18s %4d questions in %d page(s), quota left: %s",
             tag, sum(len(page["items"]) for page in pages), len(pages), pages[-1].get("quota_remaining"))
    return pages


def fetch_answers(session: requests.Session, question_ids: list[int]) -> list[dict]:
    """Answers of the given questions; the endpoint accepts up to 100 ids per request."""
    pages = []
    for start in range(0, len(question_ids), config.API_PAGE_SIZE):
        batch = ";".join(str(question_id) for question_id in question_ids[start:start + config.API_PAGE_SIZE])
        pages.extend(fetch_all_pages(session, f"questions/{batch}/answers", {"sort": "creation", "order": "asc"}))
    log.info("Answers: %d for %d answered questions in %d page(s), quota left: %s",
             sum(len(page["items"]) for page in pages), len(question_ids), len(pages),
             pages[-1].get("quota_remaining") if pages else "n/a")
    return pages


def summarize_answers(answer_pages: list[dict]) -> dict[int, dict]:
    """question_id -> creation date of its first answer and of its accepted answer (Unix seconds)."""
    summary: dict[int, dict] = {}
    for page in answer_pages:
        for answer in page["items"]:
            entry = summary.setdefault(answer["question_id"], {"first": answer["creation_date"], "accepted": None})
            entry["first"] = min(entry["first"], answer["creation_date"])
            if answer.get("is_accepted"):
                entry["accepted"] = answer["creation_date"]
    return summary


def flatten_question(item: dict, query_tag: str, answers: dict, collected_at: str) -> dict:
    """One flat record per question and query tag; values stay as delivered (dates are Unix seconds)."""
    owner = item.get("owner", {})
    return {
        "question_id": item["question_id"],
        "query_tag": query_tag,
        "title": item.get("title"),
        "tags": ";".join(item.get("tags", [])),
        "link": item.get("link"),
        "score": item.get("score"),
        "view_count": item.get("view_count"),
        "answer_count": item.get("answer_count"),
        "is_answered": item.get("is_answered"),
        "accepted_answer_id": item.get("accepted_answer_id"),
        "creation_date": item.get("creation_date"),
        "last_activity_date": item.get("last_activity_date"),
        "last_edit_date": item.get("last_edit_date"),
        "closed_date": item.get("closed_date"),
        "closed_reason": item.get("closed_reason"),
        "content_license": item.get("content_license"),
        "owner_user_id": owner.get("user_id"),
        "owner_user_type": owner.get("user_type"),
        "owner_reputation": owner.get("reputation"),
        "owner_accept_rate": owner.get("accept_rate"),
        "first_answer_date": answers.get("first"),
        "accepted_answer_date": answers.get("accepted"),
        "collected_at": collected_at,
    }


def collect() -> pd.DataFrame:
    """Collect every tag, save the untouched API responses (JSON) and the flattened records (CSV)."""
    collected_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log.info("Collecting questions tagged %s created %s .. %s", config.TAGS, config.FROM_DATE, config.TO_DATE)

    with requests.Session() as session:
        session.headers["User-Agent"] = "kbtu-dcp-sis2-pipeline/1.0"
        question_pages = {tag: fetch_questions(session, tag) for tag in config.TAGS}
        answered_ids = sorted({item["question_id"] for pages in question_pages.values() for page in pages
                               for item in page["items"] if item.get("answer_count", 0) > 0})
        answer_pages = fetch_answers(session, answered_ids)

    answers = summarize_answers(answer_pages)
    records = [flatten_question(item, tag, answers.get(item["question_id"], {}), collected_at)
               for tag, pages in question_pages.items() for page in pages for item in page["items"]]
    if not records:
        raise ApiError(f"The API returned no questions for tags {config.TAGS}")

    config.DATA_DIR.mkdir(exist_ok=True)
    with open(config.RAW_JSON_PATH, "w", encoding="utf-8") as file:
        json.dump({"collected_at": collected_at, "site": config.API_SITE, "tags": config.TAGS,
                   "from_date": str(config.FROM_DATE), "to_date": str(config.TO_DATE),
                   "question_pages": question_pages, "answer_pages": answer_pages}, file, ensure_ascii=False)

    raw_df = pd.DataFrame(records).astype({field: "Int64" for field in NULLABLE_INT_FIELDS})
    raw_df.to_csv(config.RAW_CSV_PATH, index=False)
    log.info("Collected %d records (%d unique questions, %d columns) -> %s",
             len(raw_df), raw_df["question_id"].nunique(), raw_df.shape[1], config.RAW_CSV_PATH.name)
    return raw_df


if __name__ == "__main__":
    config.setup_logging()
    try:
        collect()
    except ApiError as exc:
        log.error("Collection failed: %s", exc)
        sys.exit(1)
