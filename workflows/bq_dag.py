from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Constants
PROJECT_ID = "batchdp"
LOCATION = "US"
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# Function to read SQL
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Default args
ARGS = {
    "owner": "Ravi",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email": ["bravindraseo@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bigquery_dag",
    schedule_interval=None,
    description="DAG to run BigQuery jobs (Bronze → Silver → Gold)",
    default_args=ARGS,
    catchup=False,
    tags=["gcs", "bq", "etl", "marvel"],
) as dag:

    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION,
    )

    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION,
    )

    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        project_id=PROJECT_ID,
        location=LOCATION,
    )

    # DAG ordering
    bronze_tables >> silver_tables >> gold_tables
