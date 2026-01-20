# ДЗ 03: Проектирование DWH по модели Data Vault

## Цель

1. В результате выполнения ДЗ построить модель Data Warehouse по методу Data Vault.
2. В данном задании тренируются навыки: моделирование аналитических баз данных, моделирование по методу Data Vault.

## Решение

Исходный датасет содержит следующие поля данных:

* Ship Mode
* Segment
* Country
* City
* State
* Postal code
* Region
* Category
* Sub-category
* Sales
* Quantity
* Discount
* Profit

Выбираем несколько бизнес-ключей. Бизнес-ключи станут нашими хабами (`hub_*`), и рядим с ними будут таблицы с их описательными аттрибутами (`sat_*`):

| Бизнес ключ | Хаб | Сателлит | Аттрибуты |
|--|--|--|--|
| Доставка | `hub_ship_mode` | `sat_ship_mode_details` | **Ship Mode** |
| Сегмент потребителя | `hub_segment` | `sat_segment_details` | **Segment** |
| Категория продукта | `hub_product` | `sat_product_details` | **Category, Sub-category** |
| Локация | `hub_location` | `sat_location_details` | **Country, City, State, Postal Code, Region** |
| Транзакции | `hub_transaction` | `sat_transaction_details` | **Sales, Quantity, Discount, Profit** |

Линки (`link_sales_transaction`) - это элемент, связывающий все сущности вместе по каждой транзакции через хеш-ключи на основе измерений.

В витринах будем показывать некие сводные данные по доступным измерениям.

Таблица **cdm_sales_by_region_category** для витрины:

| Поле | Покзатель | Примечание |
|--|--|--|
| `region` | географический регион продаж | по идее можно показывать и по городам |
| `category` | основная категория продаж | |
| `sub_category` | конкретная группа товаров | |
| `transaction_count` | количество транзакций | |
| `total_sales` | общая выручка | сколько деняг принесли продажи |
| `total_profit` | общая прибыль | итоговая прибыль с продаж |
| `total_quantity` | общее количество проданного | |
| `profit_to_sales_percent` | насколько прибыльны продажи | (total_profit / total_sales) × 100% |

В этой витрине можно посмотреть, например, в каком регионе самые выгодные продажи или какая категория товаров лучше продается в конкретном регионе.

Таблица **cdm_customer_segment_analysis** для витрины:

| Поле | Покзатель | Примечание |
|--|--|--|
| `segment` | тип покупателя | |
| `region` | географический регион где покупают | по идее можно показывать и по городам |
| `ship_mode` | способ доставки, которым пользуется клиент | |
| `transaction_count` | количество транзакций | |
| `total_sales` | общая выручка | сколько деняг принесли клиенты |
| `total_profit` | общая прибыль | итоговая прибыль с клиентов |
| `avg_discount` | средняя скидка | |
| `avg_sales_per_transaction` | средний размер чека на покупку | total_sales / transaction_count |

В этой витрине можно посмотреть, например, какие категории клиентов приносят больше прибыли, какой способ доставки клиентами более предпочтительный.

SQL скрипты хранятся в каталоге `sql`:

* `sql/01_stg_setup.sql` - создание таблицы STG (Staging) слоя для сырых данных
* `sql/02_dds_setup.sql` - создание таблиц DDS (Data Delivery Service) слоя Data Vault с хабами, линками и сателлитами
* `sql/03_load_dds.sql` - перекладывание данных из STG в DDS
* `sql/04_cdm_setup.sql` - создание таблиц CDM (Common Data Model) слоя витрин для аналитики
* `sql/05_load_cdm.sql` - перекладывание данных из DDS в CDM

Сам датасет находится в `data/SampleSuperstore.csv`.

Весь пайплайн можно запустить скриптом `run_pipeline.sh`.

## Проверка

* развернуть систему в docker-compose

```bash
docker compose -f docker-compose.hw-03.yml up -d
```

* по окончании работы остановить систему командой

```bash
docker compose -f docker-compose.hw-03.yml down --remove-orphans
```

* в отдельном терминале на ПК запускаем скрипт пайплайна

```bash
./run_pipeline.sh
=== Data Vault Pipeline для данных Retail Supermarket ===
1. Создание STG таблицы...
CREATE TABLE
TRUNCATE TABLE
2. Загрузка данных в STG...
COPY 9994
3. Создание DDS структуры Data Vault...
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
CREATE TABLE
4. Загрузка данных в DDS...
CREATE FUNCTION
INSERT 0 4
INSERT 0 4
INSERT 0 3
INSERT 0 3
INSERT 0 17
INSERT 0 17
INSERT 0 632
INSERT 0 632
INSERT 0 9971
INSERT 0 9971
INSERT 0 9971
5. Создание витрин CDM...
CREATE TABLE
CREATE TABLE
6. Наполнение витрин...
INSERT 0 68
INSERT 0 48
=== Пайплайн завершен ===
```

* также можно убедиться, что произошло в БД.  
убеждаемся, что данные в таблицы загружены (специально вставил поля **loaded_at** с таймстампом операций):

```bash
psql -U postgres -d superstore_db -h localhost
Password for user postgres: 
psql (12.22 (Ubuntu 12.22-0ubuntu0.20.04.4), server 16.4)
WARNING: psql major version 12, server major version 16.
         Some psql features might not work.
Type "help" for help.

superstore_db=#
superstore_db=# \dt;
                     List of relations
 Schema |             Name              | Type  |  Owner   
--------+-------------------------------+-------+----------
 public | cdm_customer_segment_analysis | table | postgres
 public | cdm_sales_by_region_category  | table | postgres
 public | hub_location                  | table | postgres
 public | hub_product                   | table | postgres
 public | hub_segment                   | table | postgres
 public | hub_ship_mode                 | table | postgres
 public | hub_transaction               | table | postgres
 public | link_sales_transaction        | table | postgres
 public | sat_location_details          | table | postgres
 public | sat_product_details           | table | postgres
 public | sat_segment_details           | table | postgres
 public | sat_ship_mode_details         | table | postgres
 public | sat_transaction_details       | table | postgres
 public | stg_superstore                | table | postgres
(14 rows)
superstore_db=#
superstore_db=# SELECT * FROM hub_location LIMIT 3;
          hkey_location           |         bkey_location         |           loaded_at           
----------------------------------+-------------------------------+-------------------------------
 048bb5619eeb50645f3e592da8488bd7 | Henderson|Kentucky|42420      | 2025-11-17 13:03:44.611881+00
 5c39fb1d00595b66815cab25458257f5 | Los Angeles|California|90036  | 2025-11-17 13:03:44.611881+00
 0188c199e9cf404c0b7b4353e79b9c16 | Fort Lauderdale|Florida|33311 | 2025-11-17 13:03:44.611881+00
(3 rows)
superstore_db=#
superstore_db=# SELECT * FROM sat_location_details LIMIT 3;
          hkey_location           |    country    |      city       |     state      | postal_code | region |           loaded_at           
----------------------------------+---------------+-----------------+----------------+-------------+--------+-------------------------------
 048bb5619eeb50645f3e592da8488bd7 | United States | Henderson       | Kentucky       | 42420       | South  | 2025-11-17 13:03:44.771367+00
 5c39fb1d00595b66815cab25458257f5 | United States | Los Angeles     | California     | 90036       | West   | 2025-11-17 13:03:44.771367+00
 0188c199e9cf404c0b7b4353e79b9c16 | United States | Fort Lauderdale | Florida        | 33311       | South  | 2025-11-17 13:03:44.771367+00
(3 rows)
superstore_db=#
superstore_db=# SELECT * FROM cdm_customer_segment_analysis LIMIT 3;
 segment  | region  |   ship_mode    | transaction_count | total_sales | total_profit | avg_discount | avg_sales_per_transaction 
----------+---------+----------------+-------------------+-------------+--------------+--------------+---------------------------
 Consumer | Central | First Class    |               142 |    27164.03 |       971.92 |        28.07 |                    191.30
 Consumer | Central | Same Day       |                83 |    14122.34 |       832.84 |        21.33 |                    170.15
 Consumer | Central | Second Class   |               221 |    38826.15 |       778.56 |        22.88 |                    175.68
(3 rows)
superstore_db=#
superstore_db=# SELECT * FROM link_sales_transaction LIMIT 3;
         hkey_sales_link          |         hkey_transaction         |          hkey_ship_mode          |           hkey_segment           |           hkey_product           |          hkey_location           |           loaded_at           
----------------------------------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+----------------------------------+-------------------------------
 732893c498533a65b539c838c3f2ad42 | bdc6c1c4eb34a18898a01cbc5ae9a041 | 795302073614c434ae74aba2b6b78605 | f8afcf8657de570a0369d438894eed5b | 8e13f4002479c099065488f1b7ae7ad6 | 048bb5619eeb50645f3e592da8488bd7 | 2025-11-17 13:03:45.692505+00
 2b16c37a0e7c1a6fb1ca2099434d80ad | 3b56e7b7a429966c4f5ab0914b53c9c5 | 795302073614c434ae74aba2b6b78605 | f8afcf8657de570a0369d438894eed5b | 8009ed6bd4b432c50c510ab80e510a69 | 048bb5619eeb50645f3e592da8488bd7 | 2025-11-17 13:03:45.692505+00
 19a547ed049297a21667946d120fc3ab | 94008afa3fabecb84b8cb811d4c796dd | 795302073614c434ae74aba2b6b78605 | 7effe80425095de4d5b996a01e4f00a3 | 443912722eda939132e655f448610e42 | 5c39fb1d00595b66815cab25458257f5 | 2025-11-17 13:03:45.692505+00
(3 rows)
```

* можно выполнить запросы к витрине
  * топ-5 самых прибыльных подкатегорий в каждом регионе

  ```bash
  superstore_db=# SELECT region, category, sub_category, total_profit FROM cdm_sales_by_region_category ORDER BY total_profit DESC LIMIT 5;
   region  |    category     | sub_category | total_profit 
  ---------+-----------------+--------------+--------------
   West    | Technology      | Copiers      |     19327.24
   East    | Technology      | Copiers      |     17022.84
   West    | Technology      | Accessories  |     16484.60
   West    | Office Supplies | Binders      |     16096.80
   Central | Technology      | Copiers      |     15608.84
  (5 rows)
  ```

  * сравнение по среднему чеку и прибыльности в регионе

  ```bash
  superstore_db=# SELECT segment, avg_sales_per_transaction as avg_check, total_profit, (total_profit / total_sales * 100) as profit_to_sales_percent FROM cdm_customer_segment_analysis WHERE region = 'West' ORDER BY profit_to_sales_percent DESC;
     segment   | avg_check | total_profit | profit_to_sales_percent 
    -------------+-----------+--------------+-------------------------
   Consumer    |    178.07 |      4098.94 | 23.48899485228764612100
   Consumer    |    223.62 |     12176.08 | 22.59312272534803311600
   Home Office |    302.06 |      5432.61 | 19.54930806026930538300
   Home Office |    269.30 |      2026.67 | 16.72353484334502053400
   Corporate   |    281.47 |      7913.05 | 16.53727561547827388100
   Corporate   |    187.93 |      5696.15 | 15.07986745103209391000
   Corporate   |    231.53 |     18806.82 | 14.90452005076160234900
   Consumer    |    243.93 |     11742.38 | 14.72140683050168760400
   Corporate   |    341.30 |      1996.46 | 14.26738901907144699900
   Consumer    |    211.48 |     29395.79 | 13.88611743489702229100
   Home Office |    209.55 |      6495.80 |  9.45104141432438468500
   Home Office |    269.42 |      2549.04 |  9.09745978211408228300
  (12 rows)
  ```
