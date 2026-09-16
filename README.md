# Crypto Streaming Pipeline

![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE?logo=apacheairflow&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka&logoColor=white)
![Spark](https://img.shields.io/badge/Apache-Spark-E25A1C?logo=apachespark&logoColor=white)
![Cassandra](https://img.shields.io/badge/Apache-Cassandra-1287B1?logo=apachecassandra&logoColor=white)
![CI](https://github.com/Bilal51002/crypto-streaming-pipeline/actions/workflows/ci.yml/badge.svg)
![License](https://img.shields.io/badge/license-Educational-lightgrey)

Real-time cryptocurrency price streaming pipeline built with Apache Airflow, Apache Kafka (KRaft mode), Apache Spark Structured Streaming, Apache Cassandra, and a live Streamlit dashboard — fully containerized with Docker.

Live BTC, ETH, and SOL prices are pulled from the Binance public API every minute, streamed through Kafka, processed in real time by Spark, stored in Cassandra (raw records + rolling 1-minute averages), and visualized live in a Streamlit dashboard.

## Architecture

![Architecture](docs/architecture-diagram.jpeg)

```
Binance API → Airflow → Kafka (KRaft) → Spark Structured Streaming → Cassandra → Streamlit dashboard
                                                                    (raw + aggregated)
```

| Component | Role |
|---|---|
| **Binance API** | Public, no-auth-required source of live crypto prices |
| **Apache Airflow** | Orchestrates data collection every minute; publishes prices to Kafka |
| **PostgreSQL** | Stores Airflow's internal metadata (DAG runs, scheduling state) |
| **Apache Kafka (KRaft mode)** | Streaming backbone — decouples ingestion from processing. Runs without Zookeeper, using Kafka's native KRaft consensus protocol |
| **Kafka UI** | Web interface to inspect topics and messages in real time |
| **Apache Spark (Structured Streaming)** | Consumes the Kafka topic continuously via a managed `spark-job` service; writes raw prices and computes rolling 1-minute average prices per symbol |
| **Apache Cassandra** | Storage — one table for raw prices, one for 1-minute aggregates |
| **Streamlit dashboard** | Live web UI showing the latest price, price movement, and a chart per symbol, auto-refreshing every 5 seconds |
| **Docker Compose** | Orchestrates and networks all services together |
| **GitHub Actions** | CI pipeline: lints the code, builds the dashboard image, and runs a full Kafka → Spark → Cassandra integration test on every push |

### Design decisions

- **Kafka runs in KRaft mode** (no Zookeeper) — the modern, simplified way to run Kafka.
- **No Schema Registry** — a single producer/consumer pair under one person's control uses plain JSON instead of Avro, keeping the pipeline simpler without losing correctness.
- **Kafka UI replaces Confluent Control Center** — a free, lightweight alternative for topic monitoring.
- **The Spark Streaming job runs as a managed Compose service (`spark-job`)** with `restart: unless-stopped`, instead of a manual `spark-submit`, so it comes back up automatically after a `docker-compose down` / `up`.
- **The dashboard runs inside Docker, on the same network as Cassandra**, rather than connecting from the host machine. This avoids a real-world issue encountered during development: on Windows, Docker Desktop's network virtualization layer can make the Cassandra binary protocol unstable when accessed via `localhost`, even though the port itself is reachable. Connecting through Docker's internal service network (`cassandra:9042`) avoids this entirely — the same reason Spark connects the same way.
- **The Cassandra Python driver uses the `libev` connection class** instead of the default `asyncore` reactor, for a more stable long-lived connection inside the container.

## Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose
- ~4-6 GB of RAM available to Docker (Kafka, Spark, and Cassandra are memory-hungry)

## Getting started

```bash
git clone https://github.com/Bilal51002/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
docker-compose up -d
```

Give it 1-2 minutes for all services to fully initialize on first startup. The dashboard container waits for Cassandra's healthcheck before starting, and the `spark-job` service waits for Spark and Cassandra to be ready before subscribing to Kafka.

### Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Kafka UI | http://localhost:8082 | — |
| Spark Master | http://localhost:8081 | — |
| **Dashboard** | **http://localhost:8501** | — |

![Spark Master UI](docs/spark-master-ui.png)

## Running the pipeline

> Replace `<kafka-container-name>` and `<cassandra-container-name>` below with your actual container names, visible by running `docker ps` (they typically look like `<folder-name>-kafka-1`, `<folder-name>-cassandra-1`, etc.).

**1. Create the Kafka topic** (first run only):
```bash
docker exec -it <kafka-container-name> kafka-topics --create --topic crypto_prices --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

**2. Create the Cassandra keyspace and tables** (first run only):
```bash
docker exec -it <cassandra-container-name> cqlsh
```
```sql
CREATE KEYSPACE IF NOT EXISTS crypto_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE crypto_keyspace;

CREATE TABLE IF NOT EXISTS prices (
    symbol text,
    fetched_at timestamp,
    price double,
    PRIMARY KEY (symbol, fetched_at)
);

CREATE TABLE IF NOT EXISTS prices_avg_1min (
    symbol text,
    window_start timestamp,
    avg_price double,
    PRIMARY KEY (symbol, window_start)
);
```

**3. Activate the Airflow DAG** — go to the Airflow UI, find `crypto_price_stream`, and toggle it on. It runs automatically every minute, fetching BTC/ETH/SOL prices and publishing them to Kafka.

![Airflow DAG](docs/airflow-dag-ui.jpeg)

**4. The Spark Streaming job (`spark-job`) starts automatically** with `docker-compose up -d`, once Kafka, Cassandra, and the Spark cluster (`spark-master` + `spark-worker`) are ready. It consumes new Kafka messages as they arrive and writes to both Cassandra tables continuously, restarting on its own if it crashes or the stack is restarted.

**5. Open the dashboard** at http://localhost:8501 to see live prices, a live chart, and the price movement over the displayed window — refreshing automatically every 5 seconds.

![Streamlit dashboard](docs/streamlit-live-dashboard.jpeg)

You can also verify the data directly:
```bash
docker exec -it <cassandra-container-name> cqlsh -e "SELECT * FROM crypto_keyspace.prices LIMIT 10;"
docker exec -it <cassandra-container-name> cqlsh -e "SELECT * FROM crypto_keyspace.prices_avg_1min LIMIT 10;"
```

Or browse messages live in the Kafka UI at http://localhost:8082.

![Kafka UI](docs/kafka-ui-messages.jpeg)

## Continuous Integration

Every push to `main` runs a GitHub Actions pipeline (`.github/workflows/ci.yml`) with three jobs:

1. **Lint** — checks the Python code (`dashboard.py`, `dags/`, `spark/`) with `ruff`.
2. **Build** — builds the dashboard's Docker image to catch `Dockerfile` breakages early.
3. **Integration test** — spins up Kafka, Cassandra, and the Spark cluster in the CI runner, creates the topic and Cassandra tables, produces a test message, and verifies it flows all the way through to Cassandra before tearing everything down.

## Project structure

```
.
├── .github/
│   └── workflows/
│       └── ci.yml              # CI: lint, Docker build, Kafka→Spark→Cassandra integration test
├── scripts/
│   └── test_pipeline.py        # Integration test used by CI
├── docker-compose.yml          # All services and networking
├── Dockerfile                  # Image for the Streamlit dashboard
├── requirements.txt            # Python dependencies for the dashboard
├── dashboard.py                 # Streamlit dashboard: live prices, chart, auto-refresh
├── dags/
│   └── crypto_stream_dag.py    # Airflow DAG: fetches Binance prices, publishes to Kafka
├── spark/
│   └── spark_stream.py         # Spark job: consumes Kafka, writes raw + aggregated data to Cassandra
├── docs/
│   ├── architecture-diagram.jpeg
│   ├── airflow-dag-ui.jpeg
│   ├── kafka-ui-messages.jpeg
│   ├── spark-master-ui.png
│   └── streamlit-live-dashboard.jpeg
├── .gitignore
└── README.md
```

## What this project demonstrates

- Orchestrating scheduled data ingestion with Airflow
- Producing and consuming JSON messages with Kafka
- Structured Streaming with Spark: schema parsing, watermarking, windowed aggregations, and `foreachBatch` for sinks that don't natively support streaming `update` mode
- Data modeling in Cassandra with partition and clustering keys
- Building a live dashboard with Streamlit backed directly by a Cassandra data store
- Multi-container networking and debugging in Docker Compose: service name resolution, persistent volumes, container-to-container communication, healthchecks, and diagnosing a Windows-specific Docker networking issue with a binary protocol client
- Running a Spark Streaming job as a managed, self-restarting Compose service instead of a manual command
- Automated testing of a real streaming pipeline in CI: standing up Kafka, Spark, and Cassandra in a GitHub Actions runner and verifying end-to-end data flow

## Troubleshooting

**Dashboard shows `NoHostAvailable` when connecting to Cassandra**
Cassandra needs time to fully initialize (30-90s on first start). Check its healthcheck status:
```bash
docker inspect <cassandra-container-name> --format "{{.State.Health.Status}}"
```
It must show `healthy` before the dashboard can connect. If you're running the dashboard directly on your host machine (outside Docker) rather than as a Compose service, use `127.0.0.1` instead of the service name `cassandra`, and expect Windows/Docker Desktop networking to be less stable for this binary protocol than running everything inside Docker.

**Kafka topic `crypto_prices` doesn't exist / `UNKNOWN_TOPIC_OR_PARTITION`**
This happens if the `kafka_data` volume was removed (e.g. after `docker volume rm` or switching projects). Recreate the topic manually — see step 1 in "Running the pipeline".

**`spark-job` fails with `Permission denied` on `/tmp/.ivy2`**
The Spark image runs as a non-root user by default, which can't write to the `spark_ivy` volume. The service is configured with `user: "0:0"` in `docker-compose.yml` to avoid this — if you removed that line, Spark will loop on failed `mkdir` calls and never consume any Kafka messages.

**`ModuleNotFoundError: No module named 'kafka'` in the Airflow DAG**
Make sure `_PIP_ADDITIONAL_REQUIREMENTS: 'requests kafka-python'` is set on **all three** Airflow services (`airflow-init`, `airflow-webserver`, `airflow-scheduler`) in `docker-compose.yml`, not just one — each runs in its own container and needs the dependency installed independently.

## Possible next steps

- Add more trading pairs
- Add alerting on significant price movements
- Add monitoring with Prometheus and Grafana

## Author

**Bilal Khallabi**
[LinkedIn](https://linkedin.com/in/bilal-khallabi-0a1a8a315) · [GitHub](https://github.com/Bilal51002)

## License

This project is for educational purposes.
