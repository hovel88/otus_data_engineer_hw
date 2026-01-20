#!/usr/bin/env python3

'''
Скрипт загрузки данных из датасета в STG таблицу
'''

import pandas as pd
import psycopg2
import argparse
import os
from datetime import datetime

def create_parser():
    parser = argparse.ArgumentParser(formatter_class = argparse.RawTextHelpFormatter,
                                     description = __doc__)
    parser.add_argument('--host',     type = str, nargs = '?', help = 'PostgreSQL hostname', default = 'localhost',     dest='target_host')
    parser.add_argument('--user',     type = str, nargs = '?', help = 'PostgreSQL user',     default = 'postgres',      dest='target_user')
    parser.add_argument('--pass',     type = str, nargs = '?', help = 'PostgreSQL password', default = 'postgres',      dest='target_pass')
    parser.add_argument('--database', type = str, nargs = '?', help = 'PostgreSQL DB name',  default = 'superstore_db', dest='target_db')
    parser.add_argument('filename',   type = str, nargs = '+', help = 'Dataset filename')
    return parser

def load_stg_data(db_host, db_user, db_pass, db_name, dataset_file):
    conn = psycopg2.connect(
        host    =db_host,
        database=db_name,
        user    =db_user,
        password=db_pass
    )

    df = pd.read_csv(dataset_file, encoding='utf-8')

    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO stg_superstore 
            (ship_mode, segment, country, city, state, postal_code, region, category, sub_category, sales, quantity, discount, profit)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Загружено {len(df)} записей в STG слой")

def main():
    parser = create_parser()
    args = None
    try:
        args = parser.parse_args()
    except argparse.ArgumentError as err:
        print(err)
        print('Неподдерживаемая команда! Используй ({} -h)'.format(parser.prog))
        quit()
    load_stg_data(args.target_host, args.target_user, args.target_pass, args.target_db, args.filename)

if __name__ == "__main__":
    main()
