# dags/test_plugins.py
from airflow import DAG
from datetime import datetime

try:
    from plugins.sensor3.pg_sensor3 import PostgresHourlyDataSensor
    print("✅ Модуль plugins.sensor3.pg_sensor3 импортирован!")
except Exception as e:
    print("❌ Ошибка импорта:", e)

dag = DAG('test_plugins', start_date=datetime(2024, 1, 1), catchup=False)