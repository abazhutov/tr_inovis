from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup  
from datetime import datetime
from logger import write_log

TABLE_NAME = 'sales'

# Функции для логирования
def task_success(context):
    write_log(context, status=True)


def task_failure(context):
    exception = context.get("exception")
    write_log(context, status=False, error=exception)

#1. Получаем high water mark из DWH
def get_high_water_mark(ti):
    dwh_postgres = PostgresHook(postgres_conn_id="dwh_postgres")

    last_update = dwh_postgres.get_first("select coalesce(last_update, current_date)" \
        f" from high_water_mark where table_name = '{TABLE_NAME}'")[0]

    ti.xcom_push(key="last_update", value=last_update)

#2. Извлекаем данные из OPR и загружаем в MRR
def extract_to_mrr(ti):
    last_update = ti.xcom_pull(task_ids="t_get_high_water_mark", key="last_update")

    source = PostgresHook(postgres_conn_id='opr_postgres')
    target = PostgresHook(postgres_conn_id='mrr_postgres')

    rows = source.get_records("""
        select customerId, productId, qty, updated_at
        from """+TABLE_NAME+"""
        where updated_at > %s
    """, parameters=(last_update,))
    
    if not rows:
        return

    print("First 5 rows:", rows[:5])

    conn = target.get_conn()
    cursor = conn.cursor()


    cursor.execute("truncate table mrr_fact_"""+TABLE_NAME)

    insert_sql = """
        insert into mrr_fact_"""+TABLE_NAME+"""
        (customerId, productId, qty, updated_at)
        values (%s, %s, %s, %s)
    """
    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

# 3. Трансформируем (удаляем дубликаты, приводим типы, нормализуем данные, очищаем) данные в STG
def transform_to_stg():
    source = PostgresHook(postgres_conn_id='mrr_postgres')
    target = PostgresHook(postgres_conn_id='stg_postgres')
    
    rows = source.get_records("""
            select customer_id, product_id, qty, updated_at
                from (
                    select
                        customerId::bigint as customer_id,
                        productId::bigint as product_id,
                        qty::bigint,
                        updated_at::date as updated_at,
                        row_number() over (
                            partition by customerId, productId
                            order by updated_at desc
                        ) as rn
                    from mrr_fact_"""+TABLE_NAME+"""
                    where customerId is not null
                    and productId is not null
                ) t
                where rn = 1;
            """)
    
    if not rows:
        return

    conn = target.get_conn()
    cursor = conn.cursor()

    cursor.execute("truncate table stg_fact_"""+TABLE_NAME)

    insert_sql = """
        insert into stg_fact_"""+TABLE_NAME+""" (
            customer_id,
            product_id,
            qty,
            updated_at
        )
        values (%s, %s, %s, %s)
    """

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

# 4. Загружаем данные из STG в DWH (upsert - обновляем существующие записи и вставляем новые)
def load_to_dwh():
    source = PostgresHook(postgres_conn_id='stg_postgres')
    target = PostgresHook(postgres_conn_id='dwh_postgres')

    rows = source.get_records("""
        select customer_id, product_id, qty, updated_at
        from stg_fact_"""+TABLE_NAME+"""
    """)

    if not rows:
        return

    conn = target.get_conn()
    cursor = conn.cursor()

    upsert_sql = """
        insert into dwh_fact_"""+TABLE_NAME+"""
        (customer_id, product_id, qty, updated_at)
        values (%s, %s, %s, %s)
        on conflict (customer_id, product_id)
        do update set
            qty = excluded.qty
    """

    cursor.executemany(upsert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

# 5. Обновляем high water mark
def update_high_water_mark():
    dwh = PostgresHook(postgres_conn_id='dwh_postgres')

    dwh.run(f"""
        insert into high_water_mark (table_name, last_update)
        values (
            '{TABLE_NAME}',
            (select coalesce(max(updated_at), '2000-01-01') 
             from dwh_fact_{TABLE_NAME})
        )
        on conflict (table_name)
        do update set
            last_update = excluded.last_update;
    """)


with DAG(
    dag_id="tr_inovis_"+TABLE_NAME+"_etl",
    start_date=datetime(2026, 2, 17),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "on_success_callback": task_success,
        "on_failure_callback": task_failure
    }
) as dag:

    t_get_high_water_mark = PythonOperator(
        task_id='t_get_high_water_mark',
        python_callable=get_high_water_mark
    )

    extract = PythonOperator(
        task_id='extract_to_mrr',
        python_callable=extract_to_mrr
    )

    transform = PythonOperator(
        task_id='transform_to_stg',
        python_callable=transform_to_stg
    )

    load = PythonOperator(
        task_id='load_to_dwh',
        python_callable=load_to_dwh
    )

    t_update_high_water_mark = PythonOperator(
        task_id='t_update_high_water_mark',
        python_callable=update_high_water_mark
    )

    t_get_high_water_mark >> extract >> transform >> load >> t_update_high_water_mark
