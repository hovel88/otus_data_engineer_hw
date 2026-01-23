import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

conn_airflow = psycopg2.connect(
    host="postgres",
    port=5432,
    user="airflow",
    password="airflow",
    database="airflow"
)
conn_airflow.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor_airflow = conn_airflow.cursor()
cursor_airflow.execute("CREATE DATABASE analytics;")
cursor_airflow.close()

conn_airflow.close()
print("база данных 'analytics' создана")



conn_analytics = psycopg2.connect(
    host="postgres",
    port=5432,
    user="airflow",
    password="airflow",
    database="analytics"
)
conn_analytics.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cursor_analytics = conn_analytics.cursor()
cursor_analytics.execute("""
CREATE TABLE IF NOT EXISTS iss_positions (
  id            SERIAL          PRIMARY KEY,
  timestamp     BIGINT          NOT NULL,
  latitude      DECIMAL(9,6)    NOT NULL,
  longitude     DECIMAL(9,6)    NOT NULL,
  created_at    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(timestamp)
);
CREATE INDEX IF NOT EXISTS idx_timestamp ON iss_positions(timestamp);
CREATE INDEX IF NOT EXISTS idx_created_at ON iss_positions(created_at);
""")
cursor_analytics.close()

conn_analytics.close()
print("таблица 'iss_positions' создана")
