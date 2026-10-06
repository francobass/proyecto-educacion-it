"""Primer DAG de ejemplo: forma NUEVA, con el decorador @dag (TaskFlow).

La función decorada define el DAG y hay que llamarla al final del archivo.
[task1, task2] >> task3 significa: task3 corre cuando terminan task1 y task2.
"""

import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag


@dag(
    start_date=datetime.datetime(2026, 9, 1),
    catchup=False,
    schedule="@daily",
)
def primer_dag_forma_nueva():
    """Tres tareas vacías: task1 y task2 en paralelo, después task3."""
    start_task = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    end_task = EmptyOperator(task_id="task3")

    [start_task, task2] >> end_task


# Llamar a la función es lo que registra el DAG en Airflow
primer_dag_forma_nueva()
