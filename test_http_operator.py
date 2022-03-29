"""
Description:
    This DAG tests the Airflow HTTP Operator on AirHop.
"""

# import modules and functions
import json
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

# default DAG arguments
default_args = {
    "owner": "untribe",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# function to create a connection
def create_conn(**context):

    # create the Connection data
    conn = Connection(
        conn_id = "test_http_conn",
        conn_type = "http",
        host = "jsonplaceholder.typicode.com", # Free fake API for testing and prototyping
        login = "",
        password = "",
        port = None
    )

    # save the connection
    session = settings.Session()
    session.add(conn)
    session.commit()

    # return the Connection conn_id
    return conn.conn_id

# function to delete the connection
def delete_conn(**context):

    # query the DB for the "test_http_conn" connection
    session = settings.Session()
    conn = (
        session
            .query(Connection)
            .filter(Connection.conn_id == "test_http_conn")
            .one()
    )

    # delete the connection
    session.delete(conn)
    session.commit()

    # return None
    return None

# set DAG
dag = DAG(
    "test_http_operator",
    default_args = default_args,
    description = "A simple DAG to test http operator",
    schedule_interval = "@daily",
    start_date = datetime(2021, 1, 1),
    catchup = False
)

# set tasks
task_1 = PythonOperator(
    task_id = "task_create_conn",
    python_callable = create_conn,
    retries = 1,
    provide_context = True,
    dag = dag
)
task_2 = SimpleHttpOperator(
    task_id = "task_http_rest_api_request",
    http_conn_id = "test_http_conn",
    method = "GET",
    endpoint = "todos",
    data = json.dumps( {} ),
    headers = { "Content-Type": "application/json" },
    log_response = True,
    dag = dag
)
task_3 = PythonOperator(
    task_id = "task_delete_conn",
    python_callable = delete_conn,
    retries = 1,
    provide_context = True,
    dag = dag
)

# run tasks
task_1 >> task_2 >> task_3
