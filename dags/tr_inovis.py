from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup  
from datetime import datetime

def select(source_table) -> list:
    prefix = source_table[:4] if source_table[3:4] == '_' else 'opr_'

    pg_hook= PostgresHook(postgres_conn_id=prefix+"postgres")  
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {source_table}")
    rows = cursor.fetchall()
    conn.commit()
    cursor.close()
    conn.close()
    return rows

def insert(target_table, rows):
    prefix = target_table[:4] if target_table[3:4] == '_' else 'opr_'

    pg_hook = PostgresHook(postgres_conn_id=prefix+"postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for row in rows:
        sql = f"INSERT INTO {target_table} " \
        "VALUES " + str(row) + " " \
        "ON CONFLICT "

        if "sales" in target_table:
            sql = sql + "(customerId, productId) " \
            "DO UPDATE " \
            "SET qty = EXCLUDED.qty,created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at"
        elif "customers" in target_table:
            sql = sql + "(customerId) " \
            "DO UPDATE " \
            "SET qty = EXCLUDED.qty, name = EXCLUDED.name, country = EXCLUDED.country, updated_at = EXCLUDED.updated_at" 
        elif "products" in target_table:
            sql = sql + "(productId) " \
            "DO UPDATE " \
            "SET name = EXCLUDED.name, price = EXCLUDED.price, group_name = EXCLUDED.group_name, updated_at = EXCLUDED.updated_at"
        
        cursor.execute(sql, row)

    conn.commit()
    cursor.close()
    conn.close()

def extract_and_load(source_table, target_table):
    rows = select(source_table)
    insert(target_table, rows)

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

        with TaskGroup("extract") as extract:
            start = EmptyOperator(task_id="start")

            extract_and_load_mrr_sales = PythonOperator(
                task_id="extract_and_load_mrr_sales",
                python_callable=lambda: extract_and_load("sales", "mrr_fact_sales")
            )
            
            end = EmptyOperator(task_id="end")

            start >> extract_and_load_mrr_sales >> end

prepare >> extract            