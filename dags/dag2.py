from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base_hook import BaseHook

connection = BaseHook.get_connection("my_test_postgre_connectoin")

default_args = {
    "owner": "etl_user2",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 6),
}

dag = DAG('dag2', default_args=default_args, schedule_interval='0 * * * *', catchup=False,
          max_active_tasks=1, max_active_runs=1, tags=["Test2", "My two dag"])

task1 = BashOperator(
    task_id='task1',
    bash_command=(
        'python3 /opt/airflow/scripts/dag2/task1.py '
        f'--host {connection.host} '
        f'--dbname {connection.schema} '
        f'--user {connection.login} '
        f'--jdbc_password {connection.password} '
        f'--port 5432'
    ),
    dag=dag
)

task2 = BashOperator(
    task_id='task2',
    bash_command='python3 /opt/airflow/scripts/dag2/task2.py',
    dag=dag)

task1 >> task2