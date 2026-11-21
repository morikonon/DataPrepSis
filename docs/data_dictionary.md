# Data Dictionary

Source: Jolpica F1 API (Ergast-compatible), endpoint `GET https://api.jolpi.ca/ergast/f1/{season}/results/`.
Grain: **one row = one driver's result in one Grand Prix**. Seasons 2023, 2024 and 2025 (70 races, 1,398 results).
Sprint races are a separate endpoint and are not included, so points are race points only.

## 1. Collected fields (`data/raw_data.csv`, Kafka message value)

All values are kept as text exactly as the API delivered them. Missing optional fields are empty.

| Column | Type after cleaning | Kind | Description | Example |
|---|---|---|---|---|
| `record_id` | text | key | `<season>_<round>_<driver_id>`, unique per result; used as the Kafka message key | `2023_01_max_verstappen` |
| `season` | integer | categorical | Championship year | `2023` |
| `round` | integer | numerical | Race number within the season (1–24) | `1` |
| `race_name` | text | categorical | Official race name | `Bahrain Grand Prix` |
| `race_date` | date | date/time | Race day (local calendar date) | `2023-03-05` |
| `race_start_time` | – | date/time | Start time, UTC; merged into `race_datetime_utc` | `15:00:00Z` |
| `circuit_id` | text | categorical | Circuit identifier | `bahrain` |
| `circuit_name` | text | categorical | Circuit name | `Bahrain International Circuit` |
| `country` | category | categorical | Host country (UK/USA/UAE expanded to full names) | `Bahrain` |
| `locality` | text | categorical | Host city / area | `Sakhir` |
| `driver_id` | text | key | Driver identifier | `max_verstappen` |
| `driver_code` | text | categorical | Three-letter driver code | `VER` |
| `driver_number` | integer (nullable) | categorical | Permanent car number | `3` |
| `given_name`, `family_name` | – | text | Driver name; merged into `driver_name` | `Max`, `Verstappen` |
| `driver_dob` | date | date/time | Driver date of birth | `1997-09-30` |
| `driver_nationality` | category | categorical | Driver nationality | `Dutch` |
| `constructor_id` | text | key | Constructor (team entry) identifier | `red_bull` |
| `constructor_name` | category | categorical | Constructor name (" F1 Team" suffix removed) | `Red Bull` |
| `constructor_nationality` | text | categorical | Constructor nationality | `Austrian` |
| `grid` | integer | numerical | Starting position; `0` = pit-lane start | `1` |
| `position` | integer | numerical | Final position, also assigned to retired cars | `1` |
| `position_text` | text | categorical | Position as text; `R` retired, `D` disqualified, `W` withdrawn/DNS | `1` |
| `points` | float | numerical | Championship points scored in the race (0–26) | `25.0` |
| `laps` | integer | numerical | Laps completed | `57` |
| `status` | category | categorical | Finishing status (`Finished`, `Lapped`, `Retired`, `Accident`, ...) | `Finished` |
| `race_time_ms` | integer (nullable) | numerical | Total race time in milliseconds; empty when no time was recorded | `5636736` |
| `fastest_lap_rank` | integer (nullable) | numerical | Rank of the driver's fastest lap in the race | `6` |
| `fastest_lap_number` | integer (nullable) | numerical | Lap on which the fastest lap was set | `44` |
| `fastest_lap_time` | text (nullable) | numerical | Fastest lap as `m:ss.mmm` | `1:36.236` |
| `collected_at` | datetime, UTC | date/time | When the pipeline called the API | `2026-11-20T19:32:03+00:00` |

The consumer adds `kafka_partition` and `kafka_offset` (where each message was read from); they are used for
de-duplication and dropped after cleaning.

## 2. Derived fields (`data/clean_data.csv`)

| Column | Type | How it is computed |
|---|---|---|
| `driver_name` | text | `given_name + " " + family_name` |
| `race_datetime_utc` | datetime, UTC | `race_date` + `race_start_time` |
| `status_group` | category | `Finished`, `Lapped`, `DNF` (any retirement), `DNS` (did not start / withdrew), `DSQ` |
| `team_id`, `team_name` | text / category | Team lineage across renames: `alfa` → `sauber`, `alphatauri` → `rb`; name = latest name |
| `grid_effective` | integer | `grid`, but a pit-lane start (`0`) counts as the last grid slot of that race |
| `is_classified` | boolean | `position_text` is numeric (driver was classified) |
| `positions_gained` | integer (nullable) | `grid_effective − position`; empty for unclassified drivers |
| `race_time_s` | float | `race_time_ms / 1000` |
| `gap_to_winner_s` | float | Race time minus the fastest race time in the same race |
| `fastest_lap_s` | float | `fastest_lap_time` converted to seconds |
| `lap_pace_gap_pct` | float | `(fastest_lap_s / best lap of the race − 1) × 100`; comparable across circuits |
| `driver_age` | integer | Full years between `driver_dob` and `race_date` |
| `result_category` | category | `Win`, `Podium`, `Points`, `No points`, `DNF/DNS/DSQ` |

## 3. SQLite tables (`data/output.db`, schema in `sql/schema.sql`)

| Table | Primary key | Other constraints | Rows |
|---|---|---|---|
| `races` | `race_id` (`<season>_<round>`) | `UNIQUE(season, round)`, `CHECK` on season and round | 70 |
| `drivers` | `driver_id` | `CHECK(length(driver_code) = 3)` | 28 |
| `constructors` | `constructor_id` | `team_id`, `team_name` NOT NULL | 12 |
| `results` | `result_id` (= `record_id`) | `UNIQUE(race_id, driver_id)`, foreign keys to the three tables, `CHECK` on grid, position, points, laps, status_group | 1,398 |

`v_results` is a view that joins the four tables back into one row per result; the analysis reads from it.

## 4. Other output files

| File | Content |
|---|---|
| `data/raw_api_responses.json` | Untouched API responses (all pages) — the preserved original data |
| `data/consumed_data.csv` | Records as received from Kafka, plus partition and offset |
| `data/validated_data.csv` | Records that passed Pandera validation (test records excluded) |
| `data/invalid_records.csv` | Quarantined records with a `validation_errors` column |
| `data/validation_errors.csv` | One row per failed check: record, column, check, failing value |
| `data/aggregates/*.csv` | GroupBy and reshaping results (team/driver summaries, pivot tables) |
| `data/pipeline_summary.json` | Record counts of every stage and the statistics of the last run |
| `data/eda_summary.json` | Key EDA statistics |
| `logs/pipeline.log` | Log of all runs |
