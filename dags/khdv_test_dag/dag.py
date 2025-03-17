import pandas as pd
from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection("main_postgresql_connection")

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 17),
    "retries": 5,
    "retry_delay": timedelta(seconds = 5)
}

dag = DAG('dag2', default_args=default_args, schedule_interval='0 * * * *', catchup=True,
          max_active_tasks=3, max_active_runs=1, tags=["Test", "My first dag"])

task1 = BashOperator(
    task_id='task1',
    bash_command='python3 /opt/airflow/scripts/task1.py --date {{ ds }} ' +f'--host {connection.host} --dbname {connection.schema} --user {connection.login} --jdbc_password {connection.password} --port 5433',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /opt/airflow/scripts/task2.py',
    dag=dag)
task1 >> task2