-- The dataset contains sales details of different stores
-- of a supermarket chain that has multiple stores
-- in different parts of the US. With columns such as:
--  * Ship Mode
--  * Segment
--  * Country
--  * City
--  * State
--  * Postal code
--  * Region
--  * Category
--  * Sub-category
--  * Sales
--  * Quantity
--  * Discount
--  * Profit
--
-- Создание STG таблицы
CREATE TABLE IF NOT EXISTS stg_superstore (
  id            BIGSERIAL       PRIMARY KEY,
  ship_mode     VARCHAR(50)     NOT NULL,
  segment       VARCHAR(50)     NOT NULL,
  country       VARCHAR(50)     NOT NULL,
  city          VARCHAR(50)     NOT NULL,
  state         VARCHAR(50)     NOT NULL,
  postal_code   VARCHAR(50)     NOT NULL,
  region        VARCHAR(50)     NOT NULL,
  category      VARCHAR(100)    NOT NULL,
  sub_category  VARCHAR(100)    NOT NULL,
  sales         DECIMAL(10,4)   NOT NULL    DEFAULT 0,
  quantity      INTEGER         NOT NULL    DEFAULT 0,
  discount      DECIMAL(5,2)    NOT NULL    DEFAULT 0,
  profit        DECIMAL(10,4)   NOT NULL    DEFAULT 0,
  loaded_at     TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

-- Очистка таблицы перед загрузкой
TRUNCATE TABLE stg_superstore;

