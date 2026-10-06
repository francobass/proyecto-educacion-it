"""Primer DAG de ejemplo: forma CLÁSICA, con el context manager `with DAG(...)`.

Todo operador creado dentro del bloque `with` pertenece al DAG.
[task1, task2] >> task3 significa: task3 corre cuando terminan task1 y task2.
"""

import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

with DAG(
    dag_id="primer_dag_forma_vieja",
    start_date=datetime.datetime(2026, 9, 1),
    catchup=False,
    schedule="@daily",
):
    start_task = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    end_task = EmptyOperator(task_id="task3")

    [start_task, task2] >> end_task
