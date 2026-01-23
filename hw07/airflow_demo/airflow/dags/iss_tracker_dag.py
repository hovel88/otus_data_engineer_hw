from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from operators.iss_operator import IssScrapingOperator
from hooks.iss_hook import IssHook

import logging


def err_notification(context):
    print("ERROR! iteration failed")

def test_api_connection(**kwargs):
    hook = IssHook()
    if hook.service_available():
        logging.info("ISS API доступно")
        return True
    else:
        raise Exception("ISS API не доступно")

def get_short_statistic(**kwargs):
    postgres_table_name = 'iss_positions'
    pg_hook = PostgresHook(postgres_conn_id='postgres_iss_conn_id')
    sql = f"""
    SELECT
        COUNT(*) as total_records,
        MIN(created_at) as first_record,
        MAX(created_at) as last_record
    FROM {postgres_table_name};
    """
    result = pg_hook.get_first(sql)
    logging.info(f"Статистика по данным ISS:")
    logging.info(f"Всего записей   : {result[0]}")
    logging.info(f"Первая запись   : {result[1]}")
    logging.info(f"Последняя запись: {result[2]}")
    return result


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 21),
    'on_failure_callback': err_notification,
}

with DAG(
        dag_id='iss_position_tracker_dag',
        default_args=default_args,
        description='DAG для отслеживания позиции МКС',
        schedule_interval='*/30 * * * *',  # Каждые 30 минут
        catchup=False,
        tags=['iss'],
        max_active_runs=1,
) as dag:
    task_start = EmptyOperator(task_id='start')
    task_end = EmptyOperator(task_id='end')
    task_test_api = PythonOperator(task_id='test_api_connection',
                                   python_callable=test_api_connection
    )
    task_scraping = IssScrapingOperator(task_id='scraping',
                                          iss_conn_id='api_iss_conn_id',
                                          postgres_conn_id='postgres_iss_conn_id'
    )
    task_get_short_statistic = PythonOperator(task_id='get_short_statistic',
                                              python_callable=get_short_statistic
    )

    task_start >> task_test_api >> task_scraping >> task_get_short_statistic >> task_end
