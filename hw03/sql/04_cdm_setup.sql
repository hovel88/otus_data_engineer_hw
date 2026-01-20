--
-- Витрина продаж по регионам и категориям
--
CREATE TABLE IF NOT EXISTS cdm_sales_by_region_category (
    region              VARCHAR(50)     NOT NULL,
    category            VARCHAR(100)    NOT NULL,
    sub_category        VARCHAR(100)    NOT NULL,
    transaction_count   INTEGER         NOT NULL    DEFAULT 0,
    total_sales         DECIMAL(15,2)   NOT NULL    DEFAULT 0,
    total_profit        DECIMAL(15,2)   NOT NULL    DEFAULT 0,
    total_quantity      INTEGER         NOT NULL    DEFAULT 0,
    profit_to_sales_percent DECIMAL(5,2)    NOT NULL    DEFAULT 0
);

--
-- Витрина эффективности сегментов клиентов
--
CREATE TABLE IF NOT EXISTS cdm_customer_segment_analysis (
    segment             VARCHAR(50)     NOT NULL,
    region              VARCHAR(50)     NOT NULL,
    ship_mode           VARCHAR(50)     NOT NULL,
    transaction_count   INTEGER         NOT NULL    DEFAULT 0,
    total_sales         DECIMAL(15,2)   NOT NULL    DEFAULT 0,
    total_profit        DECIMAL(15,2)   NOT NULL    DEFAULT 0,
    avg_discount        DECIMAL(5,2)    NOT NULL    DEFAULT 0,
    avg_sales_per_transaction DECIMAL(10,2) NOT NULL    DEFAULT 0
);
