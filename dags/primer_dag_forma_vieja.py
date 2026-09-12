import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

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