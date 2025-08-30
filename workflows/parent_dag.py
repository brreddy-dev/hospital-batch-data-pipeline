from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator

ARGS = {
    "owner": "Ravi SEO",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email": ["***@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="parent_dag",
    schedule_interval="0 5 * * *",   # Runs daily at 5 AM
    description="Parent DAG to orchestrate PySpark & BigQuery DAGs",
    default_args=ARGS,
    catchup=False,
    tags=["parent", "orchestration", "etl"],
) as dag:

    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id="pyspark_dag",
        wait_for_completion=True,
        allowed_states=["success", "failed"],  # continue even if pyspark fails
        failed_states=[],
    )

    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id="bigquery_dag",
        wait_for_completion=True,
        trigger_rule="all_done",   # run regardless of PySpark result
    )

    trigger_pyspark_dag >> trigger_bigquery_dag
