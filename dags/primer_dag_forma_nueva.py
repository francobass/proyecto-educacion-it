import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.empty import EmptyOperator


@dag(start_date=datetime.datetime(2026, 9, 1), catchup=False, schedule="@daily",)
def primer_dag_forma_nueva():
    start_task = EmptyOperator(task_id="task1")
    task2 = EmptyOperator(task_id="task2")
    end_task = EmptyOperator(task_id="task3")

    [start_task, task2] >> end_task
    
primer_dag_forma_nueva()