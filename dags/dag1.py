from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    "start_date": datetime(2024, 7, 26),
}

dag = DAG(
    'dag1',
    default_args=default_args,
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=["Test", "My first dag"],
)

# Правильные абсолютные пути внутри контейнера
TASK1_SCRIPT_PATH = '/opt/airflow/scripts/dag1/task1.py'
TASK2_SCRIPT_PATH = '/opt/airflow/scripts/dag1/task2.py'

task1 = BashOperator(
    task_id='task1',
    bash_command=f'echo "Запуск task1..."; '
                f'ls -l {TASK1_SCRIPT_PATH} && '
                f'python3 {TASK1_SCRIPT_PATH}',
    dag=dag,
)

task2 = BashOperator(
    task_id='task2',
    bash_command=f'echo "Запуск task2..."; '
                f'ls -l {TASK2_SCRIPT_PATH} && '
                f'python3 {TASK2_SCRIPT_PATH}',
    dag=dag,
)

task1 >> task2