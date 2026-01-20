#!/bin/bash

echo "=== Data Vault Pipeline для данных Retail Supermarket ==="

# Параметры БД
# PG_HOST=postgres_host
PG_HOST=localhost
DB_NAME="superstore_db"
DB_USER="postgres"
DB_PASS="pgpass"

export PGPASSWORD=$DB_PASS

echo "1. Создание STG таблицы..."
psql -h $PG_HOST -U $DB_USER -d $DB_NAME -f sql/01_stg_setup.sql

echo "2. Загрузка данных в STG..."
# python3 data/load_stg.py --host $PG_HOST --user $DB_USER --pass $DB_PASS --database $DB_NAME SampleSuperstore.csv
psql -h $PG_HOST -U postgres -d $DB_NAME -c "
COPY stg_superstore(ship_mode, segment, country, city, state, postal_code, region, category, sub_category, sales, quantity, discount, profit)
FROM '/tmp/SampleSuperstore.csv' DELIMITER ',' CSV HEADER;"

echo "3. Создание DDS структуры Data Vault..."
psql -h $PG_HOST -U $DB_USER -d $DB_NAME -f sql/02_dds_setup.sql

echo "4. Загрузка данных в DDS..."
psql -h $PG_HOST -U $DB_USER -d $DB_NAME -f sql/03_load_dds.sql

echo "5. Создание витрин CDM..."
psql -h $PG_HOST -U $DB_USER -d $DB_NAME -f sql/04_cdm_setup.sql

echo "6. Наполнение витрин..."
psql -h $PG_HOST -U $DB_USER -d $DB_NAME -f sql/05_load_cdm.sql

echo "=== Пайплайн завершен ==="
unset PGPASSWORD
