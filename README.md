# F1 Race Results: Streaming and Validation Pipeline (Kafka + Pandera)

SIS 2, Data Collection & Preparation, KBTU.
Authors: Mukhamedali Daniyaruly, Alikhan Tursynkhan.

The pipeline collects Formula 1 race results for the 2023–2025 seasons from a public REST API, streams every record
through Apache Kafka, validates the received data with Pandera, cleans and transforms it with pandas and NumPy,
stores it in SQLite and runs an exploratory analysis with Matplotlib/Seaborn.

```
Jolpica F1 API ──> api_client.py ──> data/raw_data.csv ──> producer.py ──> Kafka topic "f1-race-results"
                                                                                │ (3 partitions, JSON)
     figures/ <── analysis.py <── data/output.db <── loader.py <── cleaner.py <── validator.py <── consumer.py
```

## 1. Requirements

| Software | Version used |
|---|---|
| Python | 3.12 |
| Docker Desktop (with Docker Compose) | 28.x — runs the Kafka broker |
| Apache Kafka (Docker image `apache/kafka`) | 3.9.1, KRaft mode, single broker |
| Python packages | see `requirements.txt` (pandas, numpy, requests, confluent-kafka, pandera, matplotlib, seaborn) |

Internet access is needed for the API call. No API key or account is required.

## 2. Installation

```bash
git clone <repository-url>
cd <repository-folder>
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

On Windows activate the environment with `.venv\Scripts\activate`.

## 3. Kafka broker

Start the broker (port `9092`) and wait until it is healthy:

```bash
docker compose up -d
docker compose ps
```

`STATUS` must show `healthy`. The broker runs in KRaft mode (no ZooKeeper); its settings are in `docker-compose.yml`.
Automatic topic creation is disabled on purpose, so the topic always has a known configuration.

Stop it with `docker compose down` (this also deletes all messages).

## 4. Kafka topic

| Setting | Value |
|---|---|
| Name | `f1-race-results` |
| Partitions | 3 |
| Replication factor | 1 (single broker) |
| `retention.ms` | 604800000 (7 days) |
| Message key | `record_id` (all copies of one result go to the same partition) |
| Message value | one result serialised as UTF-8 JSON |

The producer creates the topic automatically if it does not exist. To create or inspect it manually:

```bash
python -m src.kafka_admin create
python -m src.kafka_admin describe
docker exec f1-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic f1-race-results
docker exec f1-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group f1-pipeline-consumer
```

## 5. Configuration

Defaults are in `src/config.py`. Every value below can be overridden with an environment variable; nothing secret is
needed.

| Variable | Default | Meaning |
|---|---|---|
| `F1_API_BASE_URL` | `https://api.jolpi.ca/ergast/f1` | API base URL |
| `F1_SEASONS` | `2023,2024,2025` | Seasons to collect |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `f1-race-results` | Topic name |
| `KAFKA_GROUP_ID` | `f1-pipeline-consumer` | Consumer group |

API endpoint: `GET {F1_API_BASE_URL}/{season}/results/?limit=100&offset={n}` (100 results per page, paged by offset).

## 6. Running the pipeline

### Option A: everything with one command

```bash
python main.py
```

`main.py` runs the seven stages in order and prints a summary of record counts. Use `python main.py --skip-collect`
to reuse `data/raw_data.csv` instead of calling the API again.

### Option B: stage by stage (same order)

```bash
python -m src.api_client
python -m src.producer
python -m src.consumer --expected 1398
python -m src.validator
python -m src.cleaner
python -m src.loader
python -m src.analysis
```

| Step | Command | Reads | Writes |
|---|---|---|---|
| 1. Collect | `python -m src.api_client` | API | `data/raw_api_responses.json`, `data/raw_data.csv` |
| 2. Produce | `python -m src.producer` | `data/raw_data.csv` | Kafka topic |
| 3. Consume | `python -m src.consumer --expected N` | Kafka topic | `data/consumed_data.csv` |
| 4. Validate | `python -m src.validator` | `data/consumed_data.csv` | `data/validated_data.csv`, `data/invalid_records.csv`, `data/validation_errors.csv` |
| 5. Clean | `python -m src.cleaner` | `data/validated_data.csv` | `data/clean_data.csv`, `data/aggregates/*.csv` |
| 6. Load | `python -m src.loader` | `data/clean_data.csv` | `data/output.db` |
| 7. Analyse | `python -m src.analysis` | `data/output.db` | `figures/*.png`, `data/eda_summary.json` |

The consumer stops when it has read `--expected` messages or when no message arrives for 10 seconds. Without
`--expected` it reads everything new for its consumer group and stops on the idle timeout. It commits offsets after the
batch, so running it again only reads messages published after that.

The producer and consumer can also run at the same time in two terminals (the topic must exist, so create it first):

```bash
python -m src.kafka_admin create
python -m src.consumer --idle-timeout 60
```

and, in a second terminal while the consumer is waiting:

```bash
python -m src.producer
```

## 7. Database

The loader creates `data/output.db` from `sql/schema.sql` (tables `races`, `drivers`, `constructors`, `results`
and view `v_results`) and loads the cleaned data **twice** to show that a repeated load adds or changes nothing
(`INSERT ... ON CONFLICT DO UPDATE ... WHERE` a value differs).

```bash
python -m src.loader --verify-only
sqlite3 data/output.db "SELECT COUNT(*) FROM results;"
sqlite3 data/output.db "SELECT season, COUNT(*) FROM v_results GROUP BY season;"
```

## 8. Expected output

A run against a fresh broker ends with:

```
PIPELINE SUMMARY
  Collected from API        1398
  Published to Kafka        1398
  Consumed from Kafka       1398
  Validation input          1403
  Valid records             1398
  Invalid records              5
  Records after cleaning    1398
  Stored in SQLite          1398
  Idempotent load: True
```

The 5 invalid records are deliberately invalid test records (`*_test_*` ids) added by the validator to prove that
the schema catches errors; they are quarantined and never stored. Six figures are written to `figures/`.

## 9. Checking that it worked

1. `docker compose ps` shows the broker as `healthy`.
2. The producer log shows `Published 1398/1398 messages` and the consumer log `Consumed 1398 messages`.
3. `data/invalid_records.csv` contains exactly the 5 test records with their errors.
4. The loader log shows `Load #2: results inserted=0 updated=0` and the same fingerprint for both loads.
5. `python -m src.loader --verify-only` reports 1398 results and no rows for the duplicate, orphan and winner checks.
6. `figures/` contains `fig1` … `fig6`.

All runs are also appended to `logs/pipeline.log`.

## 10. Project structure

```
├── main.py                 runs all stages in order
├── docker-compose.yml      Kafka broker (KRaft, single node)
├── requirements.txt
├── sql/schema.sql          SQLite tables, constraints and view
├── src/
│   ├── config.py           paths and settings
│   ├── api_client.py       API collection (paging, retries, raw data)
│   ├── kafka_admin.py      broker check, topic creation/description
│   ├── producer.py         Kafka producer
│   ├── consumer.py         Kafka consumer
│   ├── validator.py        Pandera schema, test records, quarantine
│   ├── cleaner.py          cleaning, NumPy features, GroupBy, reshaping
│   ├── loader.py           SQLite load, idempotency, SQL checks
│   └── analysis.py         EDA and figures
├── data/                   raw, intermediate and final data, output.db
├── figures/                generated charts
├── docs/data_dictionary.md
├── logs/pipeline.log
└── report/project_report.pdf
```
# DataPrepSis
