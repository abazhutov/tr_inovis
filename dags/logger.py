from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
import traceback

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

def write_log(context, status: str, error: Exception | None = None):

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    
    print(f"Logging for DAG: {dag_id}, Task: {task_id}, Run: {run_id}, Status: {status}")

    start_time = context["task_instance"].start_date
    end_time = _utcnow()

    duration = (end_time - start_time).total_seconds()

    error_message = None
    if error:
        error_message = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )

    pg_hook = PostgresHook(postgres_conn_id="dwh_postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        insert into airflow_log(
            dag_id,
            task_id,
            run_id,
            status,
            start_time,
            end_time,
            duration_seconds,
            error_message
        )
        values (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        dag_id,
        task_id,
        run_id,
        status,
        start_time,
        end_time,
        duration,
        error_message
    ))

    conn.commit()
    cursor.close()
    conn.close()