from typing import Dict, Any

from airflow.hooks.base import BaseHook

import requests
import logging

class IssHook(BaseHook):
    """
    Hook для работы с API положения МКС
    """

    def __init__(self, conn_id: str = 'api_iss_conn_id'):
        super().__init__()
        self.conn_id = conn_id

    def get_url(self) -> str:
        """возвращает URL для доступа к API"""

        connection = self.get_connection(self.conn_id)
        base_url = connection.host
        url = f"{base_url}/iss-now.json"
        return url

    def service_available(self) -> bool:
        """проверяет, есть ли подключение к API сервису"""

        try:
            response = requests.get(url=self.get_url(), timeout=5)
            return response.status_code == 200
        except:
            return False

    def get_position(self) -> Dict[str, Any]:
        """получает текущую позицию МКС: Dict с данными: timestamp, latitude, longitude"""

        try:
            session = requests.Session()
            response = session.get(url=self.get_url(), timeout=10)
            response.raise_for_status()
            data = response.json()

            if data.get('message') == 'success':
                return {
                    'timestamp': data['timestamp'],
                    'latitude': float(data['iss_position']['latitude']),
                    'longitude': float(data['iss_position']['longitude']),
                    'raw_data': data
                }
            else:
                raise ValueError(f"запрос к API успешный, но вернулась ошибка: '{data}'")
        except requests.exceptions.RequestException as ex:
            logging.error(f"ошибка запроса к API: {ex}")
            raise
        except (KeyError, ValueError) as ex:
            logging.error(f"ошибка парсинга данных: {ex}")
            raise
