"""
Description:
    This DAG tests the Airflow Bash Operator on AirHop.
"""

# import modules and functions
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# default DAG arguments
default_args = {
    "owner": "untribe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# set DAG
dag = DAG(
    "test_bash_operator",
    default_args = default_args,
    description = "A simple DAG to test bash operator",
    schedule_interval = "@daily",
    start_date = datetime(2021, 1, 1),
    catchup = False
)

# set tasks
bash_task = BashOperator(
    task_id = "bash_task",
    bash_command = 'service --status-all; ps -aux; pip freeze', # list services, processes and python packages
    dag = dag
)

# run tasks
bash_task
