from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

class PostgresHourlyDataSensor(BaseSensorOperator):
    def __init__(self, postgres_conn_id: str, table_var_key: str, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_var_key = table_var_key

    def poke(self, context):
        table_name = Variable.get(self.table_var_key)
        self.log.info(f"Checking table: {table_name}")

        sql = f"""
            SELECT 1
            FROM {table_name}
            WHERE updated_at >= NOW() - interval '1 hour'
            LIMIT 1;
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        records = hook.get_records(sql)
        self.log.info(f"Records found: {records}")
        return bool(records)
