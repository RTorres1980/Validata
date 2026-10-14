from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

REPO_DIR = "/workspace"  # mount point of this repo inside the Airflow worker

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


def run_consumer(**context):
    # In production this would use KubernetesPodOperator or DockerOperator.
    subprocess.run(["python", "consumer_etl.py", "--topic", "events",
                   "--idle-timeout", "60"], cwd=REPO_DIR, check=True)


with DAG(
    dag_id="etl_with_quality_checks",
    default_args=DEFAULT_ARGS,
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    start = BashOperator(task_id="start", bash_command="echo 'Starting ETL run'")

    run_etl = PythonOperator(task_id="run_etl", python_callable=run_consumer)

    # Fails the DAG run (and triggers retries/alerts) when any expectation fails.
    validate = BashOperator(task_id="run_quality_checks",
                            bash_command=f"cd {REPO_DIR} && python quality_checks.py")

    start >> run_etl >> validate
