# Stack Overflow Questions about Data-Engineering Tools: Streaming and Validation Pipeline (Kafka + Pandera)

SIS 2, Data Collection & Preparation, KBTU.
Authors: Mukhamedali Daniyaruly, Alikhan Tursynkhan.

The pipeline collects every Stack Overflow question asked in 2025 about six data-engineering tools (pandas, Apache
Kafka, Airflow, SQLite, Selenium, web scraping) from the public Stack Exchange API, streams the records through
Apache Kafka, validates them with Pandera, cleans and transforms them with pandas and NumPy, stores them in SQLite and
analyses how often and how fast such questions get answered.

```
Stack Exchange API ──> api_client.py ──> data/raw_data.csv ──> producer.py ──> Kafka topic "so-questions"
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

Internet access is needed for the API. No account or key is required.

## 2. Installation

```bash
git clone https://github.com/morikonon/DataPrepSis.git
cd DataPrepSis
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
Automatic topic creation is disabled, so the topic always has a known configuration.

Stop it with `docker compose down` (this also deletes all messages).

## 4. Kafka topic

| Setting | Value |
|---|---|
| Name | `so-questions` |
| Partitions | 3 |
| Replication factor | 1 (single broker) |
| `retention.ms` | 604800000 (7 days) |
| Message key | `question_id` (all copies of one question go to the same partition) |
| Message value | one record serialised as UTF-8 JSON |

The producer creates the topic if it does not exist. To create or inspect it manually:

```bash
python -m src.kafka_admin create
python -m src.kafka_admin describe
docker exec so-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic so-questions
docker exec so-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group so-pipeline-consumer
```

## 5. API configuration

API documentation: <https://api.stackexchange.com/docs>. Two endpoints are used:

| Endpoint | Parameters | Returns |
|---|---|---|
| `GET https://api.stackexchange.com/2.3/questions` | `site=stackoverflow`, `tagged=<tag>`, `fromdate`, `todate` (Unix seconds), `sort=creation`, `order=asc`, `pagesize=100`, `page=N` | questions of one tag in the date window, 100 per page, `has_more` tells whether another page exists |
| `GET https://api.stackexchange.com/2.3/questions/{ids}/answers` | up to 100 question ids joined with `;`, same paging | answers of those questions (used for the first-answer and accepted-answer dates) |

Without a key the API allows **300 requests per day per IP address**; one full collection uses about 75. If the quota
runs out, register a free key at <https://stackapps.com/apps/oauth/register> and export it (it raises the quota to
10,000; never commit it):

```bash
export STACKEXCHANGE_KEY=your_key
```

All settings are in `src/config.py` and can be overridden with environment variables:

| Variable | Default | Meaning |
|---|---|---|
| `STACKEXCHANGE_KEY` | not set | optional API key |
| `SO_TAGS` | `pandas,apache-kafka,airflow,sqlite,selenium-webdriver,web-scraping` | tags to collect |
| `SO_FROM_DATE` / `SO_TO_DATE` | `2025-01-01` / `2025-12-31` | creation-date window (inclusive, UTC) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `so-questions` | topic name |
| `KAFKA_GROUP_ID` | `so-pipeline-consumer` | consumer group |

## 6. Running the pipeline

### Option A: everything with one command

```bash
python main.py
```

`main.py` runs the seven stages in order and prints a summary of record counts. `python main.py --skip-collect`
reuses `data/raw_data.csv` instead of calling the API again (saves the API quota).

### Option B: stage by stage (same order)

```bash
python -m src.api_client
python -m src.producer
python -m src.consumer --expected 2745
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

The producer and consumer can also run at the same time in two terminals (create the topic first):

```bash
python -m src.kafka_admin create
python -m src.consumer --idle-timeout 60
```

and, in a second terminal while the consumer is waiting:

```bash
python -m src.producer
```

## 7. Database

The loader creates `data/output.db` from `sql/schema.sql` (tables `questions` and `question_tags`, view
`v_question_tools`) and loads the cleaned data **twice** to show that a repeated load adds or changes nothing
(`INSERT ... ON CONFLICT DO UPDATE ... WHERE` a value differs).

```bash
python -m src.loader --verify-only
sqlite3 data/output.db "SELECT COUNT(*) FROM questions;"
sqlite3 data/output.db "SELECT tool, COUNT(*) FROM v_question_tools GROUP BY tool;"
```

## 8. Analysis and figures

`python -m src.analysis` reads the database, logs descriptive statistics and grouped results, writes
`data/eda_summary.json` and five figures to `figures/`:

| Figure | Question |
|---|---|
| `fig1_questions_per_month.png` | how the number of new questions changed during 2025 |
| `fig2_outcome_by_tool.png` | which share of each tool's questions is accepted, answered, unanswered or closed |
| `fig3_time_to_first_answer.png` | how long the first answer takes per tool |
| `fig4_reputation_effect.png` | whether the asker's reputation changes the chance of an (accepted) answer |
| `fig5_posting_heatmap.png` | on which weekdays and hours questions are posted |

## 9. Expected output

The run of 24.11.2026 against a fresh broker ended with:

```
PIPELINE SUMMARY
  Collected from API        2745
  Published to Kafka        2745
  Consumed from Kafka       2745
  Validation input          2750
  Valid records             2745
  Invalid records              5
  Records after cleaning    2649
  Stored in SQLite          2649
  Idempotent load: True
```

2745 records are 2649 questions: 96 questions carry two or three tracked tags and are returned by more than one query;
the cleaner merges them. The 5 invalid records are deliberately invalid test records (ids `900000001`–`900000005`)
added by the validator to prove that the schema catches errors; they are quarantined and never stored. Counts can
change slightly in a later run, because questions get deleted, edited and answered on the live site.

## 10. Checking that it worked

1. `docker compose ps` shows the broker as `healthy`.
2. The producer log shows `Published 2745/2745 messages` and the consumer log `Consumed 2745 messages`.
3. `data/invalid_records.csv` contains exactly the 5 test records with their errors.
4. The loader log shows `Load #2 questions inserted=0 updated=0` and the same fingerprint for both loads.
5. `python -m src.loader --verify-only` reports 2649 questions and no rows for the `questions_without_tool`,
   `tag_count_mismatch` and `inconsistent_answers` checks.
6. `figures/` contains `fig1` … `fig5`.
   

All runs are also appended to `logs/pipeline.log`.

## 11. Project structure

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
