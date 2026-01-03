from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshots for SCD2",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["dbt", "snapshots"],
) as dag:

    dbt_clean = BashOperator(
        task_id="dbt_clean",
        bash_command="cd /opt/airflow/banking_dbt && dbt clean --profiles-dir /home/airflow/.dbt"
        )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt"
    )
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt"
    )


    dbt_clean >> dbt_snapshot >> dbt_run_marts
