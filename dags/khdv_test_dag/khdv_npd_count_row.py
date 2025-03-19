import pandas as pd
import sys
from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base_hook import BaseHook
from airflow.utils.email import send_email
#from sqlalchemy import create_engine
from datetime import datetime, timedelta

connection = BaseHook.get_connection("main_postgresql_connection")
hook = PostgresHook(postgres_conn_id = 'main_postgresql_connection')

file_path = '/opt/airflow/scripts/'
include_path = '/opt/airflow/scripts/'
scripts_path = include_path + 'sql_scripts.sql'
select_path = include_path + 'select.sql'
sys.path.append(include_path)

current_date = datetime.now().strftime('%Y-%m-%d')
out_file = file_path + f'file_{current_date}.csv'

default_args = {
    "owner": "DUD_CKKD",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 17),
    "retries": 2,
    "retry_delay": timedelta(seconds = 5)
}

def execute_sql_script():
    with open(scripts_path, 'r') as file:
        sql_script = file.read()
    #hook = connection
    hook.run(sql_script)

def execute_sql_select_save(**kwargs):
    with open(select_path, 'r') as file:
        query = file.read()
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=colnames)
    #df.to_csv(out_file, index=False)
    # Проверка на пустой DataFrame
    if df.empty:
        df.to_csv(out_file, index=False)
        kwargs['ti'].xcom_push(key='is_empty', value=True)
    else:
        kwargs['ti'].xcom_push(key='is_empty', value=False)
    cursor.close()
    conn.close()

def check_df_empty(**kwargs):
    is_empty = kwargs['ti'].xcom_pull(task_ids='select_save', key='is_empty')
    return is_empty

with DAG(
     'khdv_npd_count_row',
     default_args = default_args,
     schedule_interval = None,
     catchup = True,
     description = 'Ежедневный подсчет кол-ва строк в таблицах НПД',
     max_active_tasks = 3,
     max_active_runs = 1,
     tags = ["dud_ckkd", "npd", "count_rows"]
) as dag:

    start = DummyOperator(task_id = "start", dag=dag)

    # Задача для выполнения SQL-скрипта
    run_sql_script = PythonOperator(
        task_id='run_sql_script',
        python_callable=execute_sql_script,
        dag=dag,
    )

    select_save = PythonOperator(
        task_id='select_save',
        python_callable=execute_sql_select_save,
        provide_context=True,
        dag=dag,
    )

    check_empty = BranchPythonOperator(
        task_id='check_empty',
        python_callable=check_df_empty,
        provide_context=True,
        dag=dag,
    )

    send_email = EmailOperator(
        task_id='send_email',
        to='annakhripkovafbst@gmail.com',
        subject='DAG выполнен успешно',
        html_content='DAG выполнен успешно.',
        files = [out_file],
        dag=dag,
    )

    end = DummyOperator(task_id = "end", dag=dag)

start >> run_sql_script >> select_save >> send_email >> end

