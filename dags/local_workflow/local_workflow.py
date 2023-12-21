import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval = '@monthly',
    start_date = datetime(2023, 1, 1),
    end_date = datetime(2023, 8, 1),
    catchup = True
)

with local_workflow:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "This is task 1"'
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "This is task 2"'
    )

    task1 >> task2
