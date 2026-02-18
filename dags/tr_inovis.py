from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="etl_tz_inovis",
    start_date=datetime(2026, 2, 17),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract_from_opr = PostgresOperator(
        task_id="extract_from_opr",
        postgres_conn_id="opr_postgres",
        sql="select 1 from sales limit 1;"
    )