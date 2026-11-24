# Data dictionary

Source: Stack Exchange API v2.3, site `stackoverflow`, questions created 2025-01-01 … 2025-12-31 (UTC) with one of
the tags `pandas`, `apache-kafka`, `airflow`, `sqlite`, `selenium-webdriver`, `web-scraping`.

## 1. Raw record (`data/raw_data.csv`, Kafka message value)

One record per question **and** query tag: a question with two tracked tags is returned by two API queries and
appears twice. Values are kept as delivered; dates are Unix seconds (UTC).

| Field | Type | Source (API field) | Description |
|---|---|---|---|
| `question_id` | integer | `question_id` | Stack Overflow question id; Kafka message key |
| `query_tag` | text | request parameter `tagged` | tag of the API query that returned the record |
| `title` | text | `title` | question title, HTML-escaped (`&quot;`, `&#39;`) |
| `tags` | text | `tags` | all tags of the question joined with `;` (1–5 tags) |
| `link` | text | `link` | question URL |
| `score` | integer | `score` | up-votes minus down-votes |
| `view_count` | integer | `view_count` | number of views |
| `answer_count` | integer | `answer_count` | number of (non-deleted) answers |
| `is_answered` | text `True`/`False` | `is_answered` | true if an answer is accepted or has a positive score |
| `accepted_answer_id` | integer, empty | `accepted_answer_id` | id of the accepted answer |
| `creation_date` | integer | `creation_date` | question creation time |
| `last_activity_date` | integer | `last_activity_date` | last edit, answer or comment |
| `last_edit_date` | integer, empty | `last_edit_date` | last edit of the question (empty if never edited) |
| `closed_date` | integer, empty | `closed_date` | time the question was closed |
| `closed_reason` | text, empty | `closed_reason` | close reason (Duplicate, Needs more focus, …) |
| `content_license` | text, empty | `content_license` | licence of the post (CC BY-SA 4.0) |
| `owner_user_id` | integer, empty | `owner.user_id` | asker's account id (empty for deleted accounts) |
| `owner_user_type` | text | `owner.user_type` | `registered`, `unregistered` or `does_not_exist` |
| `owner_reputation` | integer, empty | `owner.reputation` | asker's reputation |
| `owner_accept_rate` | integer, empty | `owner.accept_rate` | share of the asker's questions with an accepted answer |
| `first_answer_date` | integer, empty | `/questions/{ids}/answers` → min `creation_date` | time of the first answer |
| `accepted_answer_date` | integer, empty | `/questions/{ids}/answers` → `creation_date` where `is_accepted` | time of the accepted answer |
| `collected_at` | ISO 8601 | — | time of collection (UTC) |

The consumer adds `kafka_partition` and `kafka_offset` to every record it reads.

## 2. Cleaned data (`data/clean_data.csv`)

One row per question (duplicates merged). Dates are converted to UTC timestamps and renamed `*_date` → `*_at`.
`owner_accept_rate` (80 % empty), `content_license` (one value only) and `query_tag` (replaced by `tools`) are dropped.

| Column | Type | Description |
|---|---|---|
| `question_id` | int64 | primary key |
| `title` | string | HTML entities decoded, Unicode NFKC, single spaces |
| `tags` | string | lower-case tags joined with `;` |
| `tools` | string | tracked tools among the tags (`pandas`, `Kafka`, `Airflow`, `SQLite`, `Selenium`, `Web scraping`) |
| `tag_count`, `tool_count` | int64 | number of tags / tracked tools |
| `score`, `view_count`, `answer_count` | int64 | as in the raw data |
| `is_answered`, `is_edited`, `is_closed`, `has_accepted_answer` | bool | flags |
| `accepted_answer_id`, `owner_user_id`, `owner_reputation` | Int64 (nullable) | as in the raw data |
| `creation_at`, `last_activity_at`, `last_edit_at`, `closed_at`, `first_answer_at`, `accepted_answer_at`, `collected_at` | datetime (UTC) | converted from Unix seconds |
| `closed_reason`, `owner_user_type` | string | as in the raw data |
| `hours_to_first_answer`, `hours_to_accepted_answer` | float | hours from the question to the answer (NULL without one) |
| `answered_within_24h` | bool | first answer within 24 hours |
| `views_per_day` | float | `view_count` / days between creation and collection |
| `log_views` | float | `log(1 + view_count)` |
| `status` | string | `Closed`, `Accepted`, `Answered, not accepted`, `Unanswered` (checked in this order) |
| `reputation_band` | string | `1-9`, `10-99`, `100-999`, `1000+`, `Deleted account` |
| `creation_month` | string | `YYYY-MM` |
| `creation_weekday` | string | Monday … Sunday |
| `creation_hour_utc` | int | 0–23 |
| `title_length` | int64 | characters in the cleaned title |

### Missing values after cleaning

All remaining gaps are structural and stay NULL; filling them would invent events that never happened.

| Column | Missing means |
|---|---|
| `closed_at`, `closed_reason` | the question is open |
| `accepted_answer_id`, `accepted_answer_at`, `hours_to_accepted_answer` | no accepted answer |
| `last_edit_at` | never edited |
| `first_answer_at`, `hours_to_first_answer` | no answers |
| `owner_user_id`, `owner_reputation` | the asker deleted the account |

## 3. Database (`data/output.db`, created by `sql/schema.sql`)

| Object | Key | Content |
|---|---|---|
| `questions` | `question_id` (PRIMARY KEY), `link` UNIQUE | all columns of the cleaned data except `tags` and `tools`; dates as ISO 8601 text, flags as 0/1 |
| `question_tags` | (`question_id`, `tag`) PRIMARY KEY, `question_id` → `questions` | one row per tag of a question; `tag_position` 1–5; `tool` = tool name for tracked tags, otherwise NULL |
| `v_question_tools` | view | `questions` joined with its tracked tools: one row per question and tool |

## 4. Aggregates (`data/aggregates/`)

| File | Built with | Content |
|---|---|---|
| `tool_summary.csv` | `groupby().agg()` | questions, answered / accepted / closed rates, median hours to the first answer, median views per tool |
| `questions_by_month.csv` | `pivot_table()` | month × tool question counts |
| `questions_by_month_long.csv` | `melt()` | the same table in long format |
| `status_by_tool.csv` | `groupby().size().unstack()` | tool × status counts |
| `unanswered_popular.csv` | filter with three conditions | unanswered, not down-voted questions with views ≥ the 75th percentile |
