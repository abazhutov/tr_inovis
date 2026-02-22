from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup  
from datetime import datetime

TABLE_NAME = 'customers'

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
        select id, name, country, updated_at
        from """+TABLE_NAME+"""
        where updated_at > %s
    """, parameters=(last_update,))
    
    if not rows:
        return

    print("First 5 rows:", rows[:5])

    conn = target.get_conn()
    cursor = conn.cursor()


    cursor.execute("truncate table mrr_dim_"""+TABLE_NAME)

    insert_sql = """
        insert into mrr_dim_"""+TABLE_NAME+"""
        (id, name, country, updated_at)
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
            select id, name, country, updated_at
                from (
                    select
                        id::int,
                        name::text,
                        country::text,
                        updated_at::date,
                        row_number() over (
                            partition by id
                            order by updated_at desc
                        ) as rn
                    from mrr_dim_"""+TABLE_NAME+"""
                    where id is not null
                    and name is not null
                    and country is not null
                ) t
                where rn = 1;
            """)
    
    if not rows:
        return

    conn = target.get_conn()
    cursor = conn.cursor()

    cursor.execute("truncate table stg_dim_"""+TABLE_NAME)

    insert_sql = """
        insert into stg_dim_"""+TABLE_NAME+""" (
            id,
            name,
            country,
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
        select id, name, country, updated_at
        from stg_dim_"""+TABLE_NAME+"""
    """)

    if not rows:
        return

    conn = target.get_conn()
    cursor = conn.cursor()

    upsert_sql = """
        insert into dwh_dim_"""+TABLE_NAME+"""
        (id, name, country, updated_at)
        values (%s, %s, %s, %s)
        on conflict (id)
        do update set
            name = excluded.name,
            country = excluded.country,
            updated_at = excluded.updated_at
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
             from dwh_dim_{TABLE_NAME})
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
