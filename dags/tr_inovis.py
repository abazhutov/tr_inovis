from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup  
from datetime import datetime

with DAG(
    dag_id="tr_inovis",
    start_date=datetime(2026, 2, 17),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow/sql"]

 ) as dag:
        
        with TaskGroup("prepare") as prepare:
            start = EmptyOperator(task_id="start")

            # Создаем таблицы в mrr, stg и dwh
            create_tables_mrr = PostgresOperator(
                task_id="create_tables_mrr",
                postgres_conn_id="mrr_postgres",
                sql="create_tables_mrr.sql"
            )
            create_tables_stg = PostgresOperator(
                task_id="create_tables_stg",
                postgres_conn_id="stg_postgres",
                sql="create_tables_stg.sql"
            )
            create_tables_dwh = PostgresOperator(
                task_id="create_tables_dwh",
                postgres_conn_id="dwh_postgres",
                sql="create_tables_dwh.sql"
            )
            end = EmptyOperator(task_id="end")
            
            start >> [create_tables_mrr, create_tables_stg, create_tables_dwh] >> end