from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup  
from datetime import datetime

def get_last_updated():
    pg_hook = PostgresHook(postgres_conn_id="dwh_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("select max(coalesce(last_updated, '1900-01-01')) "\
                   "from dwh_high_water_mark where table_name = 'dwh_high_water_mark'")
    last_updated = cursor.fetchall()[0][0]   
    cursor.close()
    conn.close()
    return last_updated

def select(source_table) -> list:
    prefix = source_table[:4] if source_table[3:4] == '_' else 'opr_'

    pg_hook= PostgresHook(postgres_conn_id=prefix+"postgres")  
    conn = pg_hook.get_conn()
    cursor = conn.cursor()  

    cursor.execute(f"SELECT * FROM {source_table} " \
                "where updated_at > %s", (get_last_updated(),))
    
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
        if "sales" in target_table:
            customerId, productId, qty, created_at, updated_at = row
            sql =  f"INSERT INTO {target_table} " \
                "VALUES (%s, %s, %s, %s, %s) " \
                "ON CONFLICT (customerId, productId) " \
                "DO UPDATE " \
                "SET qty = EXCLUDED.qty,created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at"
            cursor.execute(sql, [customerId, productId, qty, created_at, updated_at])
        elif "customers" in target_table:
            customerId, name, country, created_at, updated_at = row
            sql =  f"INSERT INTO {target_table} " \
                "VALUES (%s, %s, %s, %s, %s) "\
                "ON CONFLICT (customerId) " \
                "DO UPDATE " \
                "SET name = EXCLUDED.name, country = EXCLUDED.country, updated_at = EXCLUDED.updated_at" 
            cursor.execute(sql, [customerId, name, country, created_at, updated_at])
        elif "products" in target_table:
            productId, name, price, group_name, created_at, updated_at = row
            sql =  f"INSERT INTO {target_table} "\
                "VALUES (%s, %s, %s, %s, %s, %s) "\
                "ON CONFLICT (productId) " \
                "DO UPDATE " \
                "SET name = EXCLUDED.name, price = EXCLUDED.price, group_name = EXCLUDED.group_name, updated_at = EXCLUDED.updated_at"
            cursor.execute(sql, [productId, name, price, group_name, created_at, updated_at])

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

            extract_and_load_stg_sales = PythonOperator(
                task_id="extract_and_load_stg_sales",
                python_callable=lambda: extract_and_load("mrr_fact_sales", "stg_fact_sales")
            )

            extract_and_load_dwh_sales = PythonOperator(
                task_id="extract_and_load_dwh_sales",
                python_callable=lambda: extract_and_load("stg_fact_sales", "dwh_fact_sales")
            )
            
            end = EmptyOperator(task_id="end")

            start >> extract_and_load_mrr_sales >> extract_and_load_stg_sales >> extract_and_load_dwh_sales >> end

prepare >> extract           