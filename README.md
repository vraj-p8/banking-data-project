# Banking Data Pipeline

End-to-end CDC pipeline that moves banking data from Postgres into Snowflake and models it with dbt. It uses Debezium + Kafka for change capture, MinIO as a landing zone, and Airflow to orchestrate loads and dbt snapshots.

## Data flow
1. Fake data generator writes to Postgres.
2. Debezium streams changes into Kafka topics.
3. Consumer batches Kafka events into Parquet and uploads to MinIO.
4. MinIO sends a webhook to the FastAPI event receiver.
5. Event receiver triggers an Airflow DAG to load files into Snowflake RAW tables.
6. Airflow triggers dbt snapshots and marts.

## Stack
- Postgres, Debezium, Kafka, MinIO
- Airflow + dbt (Snowflake adapter)
- FastAPI event receiver
- Docker Compose for local orchestration

## Quick start
1. Update environment files:
   - `.env` for Docker Compose (Postgres, MinIO, Airflow, FastAPI)
   - `docker/dags/.env` for Airflow DAG secrets (MinIO + Snowflake)
   - `kafka-debezium/.env`, `consumer/.env`, `data-generator/.env` for local scripts
2. Start the stack:
   - `docker compose up -d --build`
3. Create the Debezium connector:
   - `python kafka-debezium/generate_and_post_connector.py`
4. Generate data:
   - `python data-generator/faker_generator.py --once` (omit `--once` to loop)
5. Start the Kafka -> MinIO consumer:
   - `python consumer/kafka_to_minio.py`
6. Watch the pipeline:
   - MinIO uploads trigger Airflow automatically via the event receiver.

## Expected Postgres schema
Minimum columns used by the generator and CDC:
```sql
CREATE TABLE IF NOT EXISTS customers (
   id SERIAL PRIMARY KEY,
   first_name VARCHAR(100) NOT NULL,
   last_name VARCHAR(100) NOT NULL,
   email VARCHAR(255) UNIQUE NOT NULL,
   created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS accounts (
   id SERIAL PRIMARY KEY,
   customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
   account_type VARCHAR(50) NOT NULL,
   balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
   currency CHAR(3) NOT NULL DEFAULT 'USD',
   created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
   id BIGSERIAL PRIMARY KEY,
   account_id INT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
   txn_type VARCHAR(50) NOT NULL, -- DEPOSIT | WITHDRAWAL | TRANSFER
   amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
   related_account_id INT NULL, -- for transfers
   status VARCHAR(20) NOT NULL DEFAULT 'COMPLETED',
   created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);
```

## Useful URLs
- Airflow UI: http://localhost:8080
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001
- Debezium Connect: http://localhost:8083

## Repo layout
- `app.py` and `run.py`: FastAPI event receiver.
- `docker-compose.yml`: Local stack (Kafka, Debezium, Postgres, MinIO, Airflow).
- `data-generator/`: Inserts fake banking data into Postgres.
- `kafka-debezium/`: Creates the Debezium connector.
- `consumer/`: Reads Kafka topics and writes Parquet to MinIO.
- `docker/dags/`: Airflow DAGs and Airflow env file.
- `banking_dbt/`: dbt project (snapshots + marts).

## CI/CD
- CI (`.github/workflows/ci.yml`): linting, unit tests, and dbt compile.
- CD (`.github/workflows/cd.yml`): dbt run + test on `main`.
