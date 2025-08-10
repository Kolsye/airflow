from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.models import Variable
from sensor3.pg_sensor3 import PostgresHourlyDataSensor
from airflow.hooks.base_hook import BaseHook
import clickhouse_connect


def create_clickhouse_database_if_not_exists(client, database_name):
    # Проверяем, существует ли база данных
    try:
        client.command(f"USE {database_name}")
    except Exception as e:
        # Если база данных не существует — создаём её
        client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")

def create_clickhouse_table_if_not_exists(client, table_name):
    # Сначала убедимся, что база данных существует
    create_clickhouse_database_if_not_exists(client, "dwh_click")

    # Теперь создадим таблицу
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id UInt64,
        token_name String,
        currency String,
        period String,
        price Float64,
        volatility Float64,
        updated_at DateTime
    ) ENGINE = MergeTree()
    ORDER BY (id, updated_at)
    """
    client.command(create_table_sql)

def etl_task():
    table_name = Variable.get("source_table_name")  # например, 'crypto_prices_2'
    pg_conn_id = "my_test_postgre_connection"

    # Получаем конфигурацию подключения
    pg_hook = BaseHook.get_connection(pg_conn_id)

    # Подключаемся к Postgres
    import psycopg2
    conn = psycopg2.connect(
        host=pg_hook.host,
        port=pg_hook.port,
        user=pg_hook.login,
        password=pg_hook.password,
        dbname=pg_hook.schema or "postgres"
    )
    cur = conn.cursor()

    # ✅ Исправлено: используем updated_at, а не created_at
    cur.execute(f"""
        SELECT id, token_name, currency, period, price, volatility, updated_at
        FROM {table_name}
        WHERE updated_at >= NOW() - INTERVAL '1 hour'
    """)
    records = cur.fetchall()
    cur.close()
    conn.close()

    # Чистим данные (убираем строки с NULL)
    cleaned = [row for row in records if None not in row]

    # Подключаемся к ClickHouse
    clickhouse_client = clickhouse_connect.get_client(
        host='10.19.88.103',
        port=8123,
        username='tech_load',
        password='dwh88'
    )

    # Создаём таблицу, если её нет
    create_clickhouse_table_if_not_exists(clickhouse_client, "dwh_click.crypto_prices_2_analytics")

    # ✅ Вставляем все поля
    if cleaned:
        clickhouse_client.insert(
            'dwh_click.crypto_prices_2_analytics',
            cleaned,
            column_names=[
                'id', 'token_name', 'currency', 'period', 'price', 'volatility', 'updated_at'
            ]
        )


with DAG(
    dag_id='pg_to_clickhouse_sensor_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['sensor', 'clickhouse'],
) as dag:

    wait_for_pg_data = PostgresHourlyDataSensor(
        task_id='wait_for_hourly_pg_data',
        postgres_conn_id='my_test_postgre_connection',
        table_var_key='source_table_name',
        poke_interval=300,
        timeout=3600,
    )

    run_etl = PythonOperator(
        task_id='run_etl_task',
        python_callable=etl_task
    )

    wait_for_pg_data >> run_etl
