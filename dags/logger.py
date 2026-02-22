from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional, Dict
import traceback
import json

from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "dwh_postgres"


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_exception(error: Exception) -> str:
    return json.dumps(
        {
            "type": type(error).__name__,
            "message": str(error),
            "traceback": traceback.format_exception(
                type(error), error, error.__traceback__
            ),
        },
        ensure_ascii=False,
    )


def write_log(
    context: Dict[str, Any],
    status: str,
    error: Optional[Exception] = None,
) -> None:
    """
    Writes execution metadata of a task into etl_process_log table.
    """

    ti = context["task_instance"]
    dag = context["dag"]

    dag_id: str = dag.dag_id
    task_id: str = ti.task_id
    run_id: str = context["run_id"]

    start_time: datetime = ti.start_date or _utcnow()
    end_time: datetime = _utcnow()

    duration_seconds: float = (
        end_time - start_time
    ).total_seconds()

    error_payload: Optional[str] = (
        _serialize_exception(error) if error else None
    )

    sql = """
        insert into etl_process_log (
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
    """

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    dag_id,
                    task_id,
                    run_id,
                    status,
                    start_time,
                    end_time,
                    duration_seconds,
                    error_payload,
                ),
            )
        conn.commit()