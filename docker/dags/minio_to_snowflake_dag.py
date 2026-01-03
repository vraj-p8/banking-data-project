import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
env_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(env_path)

# -------- MinIO Config --------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

# -------- Snowflake Config --------
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]

# -------- Python Callables --------
def _extract_keys(conf):
    keys = []
    if not conf or not isinstance(conf, dict):
        return keys
    if isinstance(conf.get("keys"), list):
        keys.extend([k for k in conf["keys"] if k])
    if conf.get("key"):
        keys.append(conf["key"])
    return keys


def download_from_minio(**context):
    os.makedirs(LOCAL_DIR, exist_ok=True)
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    keys = _extract_keys(conf)
    bucket = conf.get("bucket") if isinstance(conf, dict) else None
    bucket = bucket or BUCKET

    local_files = {table: [] for table in TABLES}

    if not keys:
        for table in TABLES:
            prefix = f"{table}/"
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            objects = resp.get("Contents", [])
            for obj in objects:
                key = obj["Key"]
                local_name = key.replace("/", "_")
                local_file = os.path.join(LOCAL_DIR, local_name)
                s3.download_file(bucket, key, local_file)
                print(f"Downloaded {key} -> {local_file}")
                local_files[table].append(local_file)
        return local_files

    for key in keys:
        table = key.split("/", 1)[0]
        if table not in TABLES:
            print(f"Skipping unknown table prefix for key: {key}")
            continue
        local_name = key.replace("/", "_")
        local_file = os.path.join(LOCAL_DIR, local_name)
        s3.download_file(bucket, key, local_file)
        print(f"Downloaded {key} -> {local_file}")
        local_files[table].append(local_file)
    return local_files

def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print("No files found in MinIO.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )
    cur = conn.cursor()

    for table, files in local_files.items():
        if not files:
            print(f"No files for {table}, skipping.")
            continue

        for f in files:
            stage_file = os.path.basename(f)
            cur.execute(f"PUT file://{f} @%{table} AUTO_COMPRESS=FALSE")
            print(f"Uploaded {f} -> @{table} stage")

            copy_sql = f"""
            COPY INTO {table}
            FROM @%{table}
            FILES = ('{stage_file}')
            FILE_FORMAT=(TYPE=PARQUET)
            ON_ERROR='CONTINUE'
            """
            cur.execute(copy_sql)
            cur.execute(f"REMOVE @%{table} pattern='.*{stage_file}.*'")
            os.remove(f)
            print(f"Data loaded into {table} from {stage_file}")

    cur.close()
    conn.close()

def has_files(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio") or {}
    return any(files for files in local_files.values())

# -------- Airflow DAG --------
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    description="Load MinIO parquet into Snowflake RAW tables",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    task1 = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    task_check = ShortCircuitOperator(
        task_id="has_files",
        python_callable=has_files,
        provide_context=True,
    )

    task2 = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_snapshots",
        trigger_dag_id="SCD2_snapshots",
        wait_for_completion=False,
    )

    task1 >> task_check >> task2 >> trigger_dbt
