from typing import Dict, Any

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from hooks.iss_hook import IssHook

import logging

class IssScrapingOperator(BaseOperator):
    """
    Operator для получения данных положения МКС и сохранения их в PostgreSQL
    """

    def __init__(
        self,
        iss_conn_id: str = 'api_iss_conn_id',
        postgres_conn_id: str = 'postgres_iss_conn_id',
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.iss_conn_id = iss_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.postgres_table_name = 'iss_positions'

    def execute(self, context: Dict[str, Any]):
        logging.info(f"выполнение IssScrapingOperator...")

        iss_hook = IssHook(conn_id=self.iss_conn_id)
        pg_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        logging.info(f"считывание данных ISS из '{iss_hook.get_url()}'")
        iss_data = iss_hook.get_position()
        logging.info(f"данные ISS получены: {iss_data}")

        try:
            insert_sql = f"""
            INSERT INTO {self.postgres_table_name} (timestamp, latitude, longitude)
                 VALUES (%s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
            """
            insert_params = (
                iss_data['timestamp'],
                iss_data['latitude'],
                iss_data['longitude']
            )
            pg_hook.run(insert_sql, parameters=insert_params)
            logging.info(f"данные сохранены в таблицу {self.postgres_table_name}")
        except Exception as ex:
            logging.error(f"произошла ошибка при сохранении в PostgreSQL: {ex}")
            raise

        return iss_data
