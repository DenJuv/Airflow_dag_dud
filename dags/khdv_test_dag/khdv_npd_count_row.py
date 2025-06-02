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
import pytz
import os
import logging

#connection = BaseHook.get_connection("main_postgresql_connection")
hook = PostgresHook(postgres_conn_id = 'main_postgresql_connection')

file_path = '/opt/airflow/scripts/'
include_path = '/opt/airflow/scripts/'
scripts_path = include_path + 'sql_scripts.sql'
select_path = include_path + 'select.sql'
sys.path.append(include_path)
timezone = pytz.timezone('Europe/Moscow')
current_date = datetime.now(timezone).strftime('%Y-%m-%d')
current_date_time = datetime.now(timezone).strftime('%Y-%m-%d %H:%M:%S')
out_file = file_path + f'khdv_npd_table_row_{current_date}.csv'

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

    #logging.info("\n" +'LOOOOOOOOOOOOOG - >'+ df.head(10).to_string())
    if df.empty:
        kwargs['ti'].xcom_push(key='is_empty', value=True)
    else:
        df.to_csv(out_file, index=False)
        kwargs['ti'].xcom_push(key='is_empty', value=False)
    cursor.close()
    conn.close()

def check_df_empty(**kwargs):
    is_empty = kwargs['ti'].xcom_pull(task_ids='select_save_task', key='is_empty')
    if is_empty:
        return 'send_email_null_task'  # Если DataFrame пустой, переход к таске
    else:
        return 'send_email_notnull_task'  # Если DataFrame не пустой, переходим

def delete_file_if_exists():
    if os.path.exists(out_file):
        os.remove(out_file)
        print(f'Файл {out_file} успешно удален.')
    else:
        print(f'Файл {out_file} не существует.')

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

    start = DummyOperator(task_id = "start", dag=dag,)

    sql_script_task = PythonOperator(
        task_id='sql_script_task',
        python_callable=execute_sql_script,
        dag=dag,
    )

    select_save_task = PythonOperator(
        task_id='select_save_task',
        python_callable=execute_sql_select_save,
        provide_context=True,
        dag=dag,
    )

    check_empty_task = BranchPythonOperator(
        task_id='check_empty_task',
        python_callable=check_df_empty,
        provide_context=True,
        dag=dag,
    )

    send_email_notnull_task = EmailOperator(
        task_id='send_email_notnull_task',
        to='annakhripkovafbst@gmail.com',
        subject='DAG отработал успешно',
        html_content=f'''
            DAG отработал корректно в : - {current_date_time} .<br>
            файл с добавленными строками во вложении
        ''',
        files = [out_file],
        dag=dag,
    )

    send_email_null_task = EmailOperator(
        task_id='send_email_null_task',
        to='annakhripkovafbst@gmail.com',
        subject='DAG отработал с ошибкой!',
        html_content=f'DAG отработал с ошибкой: - {current_date_time}',
        dag=dag,
    )

    merge_tasks = DummyOperator(task_id='merge_tasks', trigger_rule="none_failed_or_skipped", dag=dag,)

    delete_file_task = PythonOperator(
        task_id='delete_file_task',
        python_callable=delete_file_if_exists,
        dag=dag,
    )

    end = DummyOperator(task_id = "end", dag=dag,)

start >> sql_script_task >> select_save_task >> check_empty_task
check_empty_task >> send_email_notnull_task >> merge_tasks
check_empty_task >> send_email_null_task >> merge_tasks
merge_tasks >> delete_file_task >> end


