from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator

# Убедитесь, что start_date не в будущем, если вы хотите, чтобы DAG запускался сразу.
# Или установите его в прошлое, если используете catchup=False.
default_args = {
    "owner": "etl_user",
    "depends_on_past": False,
    # Рекомендуется использовать дату в прошлом или настоящем
    "start_date": datetime(2024, 7, 26), # datetime(2025, 7, 26)
}
# изменения 1
# Установите catchup=False, если не хотите запускать пропущенные интервалы из прошлого.
dag = DAG(
    'dag1',
    default_args=default_args,
    schedule_interval='0 * * * *', # Каждый час в 00 минут
    catchup=False, # Не запускать пропущенные интервалы
    max_active_tasks=1,
    max_active_runs=1,
    tags=["Test", "My first dag"],
    # Рекомендуется явно указать dagrun_timeout
    # dagrun_timeout=timedelta(minutes=30)
)

# Используйте АБСОЛЮТНЫЕ пути к скриптам
TASK1_SCRIPT_PATH = '/root/airflow/scripts/dag1/task1.py'
TASK2_SCRIPT_PATH = '/root/airflow/scripts/dag1/task2.py'

task1 = BashOperator(
    task_id='task1',
    # Добавим проверку существования файла и более информативный вывод
    bash_command=f'echo "Запуск task1..."; ls -l {TASK1_SCRIPT_PATH}; python3 {TASK1_SCRIPT_PATH}',
    dag=dag)

task2 = BashOperator(
    task_id='task2',
    bash_command=f'echo "Запуск task2..."; ls -l {TASK2_SCRIPT_PATH}; python3 {TASK2_SCRIPT_PATH}',
    dag=dag)

task1 >> task2