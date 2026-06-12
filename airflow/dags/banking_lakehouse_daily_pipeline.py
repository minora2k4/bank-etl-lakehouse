from datetime import datetime

try:
    from airflow import DAG
    from airflow.operators.bash import BashOperator
except ImportError:
    DAG = None
    BashOperator = None

if DAG:
    with DAG(
        dag_id="banking_lakehouse_daily_pipeline",
        start_date=datetime(2026, 6, 10),
        schedule="@daily",
        catchup=False,
        tags=["banking", "lakehouse", "data_quality"],
    ) as dag:
        run_pipeline = BashOperator(
            task_id="run_banking_lakehouse_pipeline",
            bash_command='cd "${PROJECT_HOME:-/opt/airflow/project}" && sh scripts/pipeline.sh ',
        )
