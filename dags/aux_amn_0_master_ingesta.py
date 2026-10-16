"""DAG maestro de la ingesta AMN (seed de la base origen amn).

Dispara en orden los 3 DAGs seed y espera a que cada uno termine antes de seguir:
    aux_amn_1_data_ingestion_dag -> aux_amn_2_data_transformation_dag -> aux_amn_3_database_loader_dag

Se ejecuta a mano. Los 3 DAGs disparados tienen que estar activados (unpaused) para que corran:
si uno está pausado, su corrida queda en "queued" y este DAG espera sin dar error hasta que lo actives.
"""

from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG

with DAG(
    "aux_amn_0_master_ingesta",
    description="Orquesta la ingesta AMN: ingestion -> transformation -> loader",
    schedule=None,  # sólo se ejecuta a mano
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    # wait_for_completion=True: la tarea espera a que termine el DAG disparado
    # (y falla si ese DAG falla), así la siguiente no arranca antes de tiempo.
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="aux_amn_1_data_ingestion_dag",
        wait_for_completion=True,
        poke_interval=10,  # cada cuántos segundos revisa si terminó
    )
    trigger_transformation = TriggerDagRunOperator(
        task_id="trigger_transformation",
        trigger_dag_id="aux_amn_2_data_transformation_dag",
        wait_for_completion=True,
        poke_interval=10,
    )
    trigger_load = TriggerDagRunOperator(
        task_id="trigger_load",
        trigger_dag_id="aux_amn_3_database_loader_dag",
        wait_for_completion=True,
        poke_interval=10,
    )

    trigger_ingestion >> trigger_transformation >> trigger_load
