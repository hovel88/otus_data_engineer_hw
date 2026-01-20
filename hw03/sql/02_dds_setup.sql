--
-- ХАБЫ (Hub) - бизнес-ключи
--

CREATE TABLE IF NOT EXISTS hub_ship_mode (
  hkey_ship_mode    TEXT            PRIMARY KEY,        -- hash key: MD5([ship_mode])
  bkey_ship_mode    TEXT            NOT NULL    UNIQUE, -- business key: ship_mode
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hub_segment (
  hkey_segment      TEXT            PRIMARY KEY,        -- hash key: MD5([segment])
  bkey_segment      TEXT            NOT NULL    UNIQUE, -- business key: segment
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hub_product (
  hkey_product      TEXT            PRIMARY KEY,        -- hash key: MD5([category,sub_category])
  bkey_product      TEXT            NOT NULL    UNIQUE, -- business key: category|sub_category
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hub_location (
  hkey_location     TEXT            PRIMARY KEY,        -- hash key: MD5([city,state,postal_code])
  bkey_location     TEXT            NOT NULL    UNIQUE, -- business key: city|state|postal_code
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hub_transaction (
  hkey_transaction  TEXT            PRIMARY KEY,        -- hash key: MD5([city,state,postal_code,category,sub_category,segment,ship_mode,sales,quantity,discount])
  bkey_transaction  TEXT            NOT NULL    UNIQUE, -- business key: city|state|postal_code|category|sub_category|segment|ship_mode|sales|quantity|discount
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);


--
-- САТЕЛЛИТЫ (Satellite) - описательные атрибуты
--

CREATE TABLE IF NOT EXISTS sat_ship_mode_details (
  hkey_ship_mode    TEXT            PRIMARY KEY,    -- hash key: MD5([ship_mode])
  ship_mode         VARCHAR(50)     NOT NULL,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sat_segment_details (
  hkey_segment      TEXT            PRIMARY KEY,    -- hash key: MD5([segment])
  segment           VARCHAR(50)     NOT NULL,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sat_product_details (
  hkey_product      TEXT            PRIMARY KEY,    -- hash key: MD5([category,sub_category])
  category          VARCHAR(100)    NOT NULL,
  sub_category      VARCHAR(100)    NOT NULL,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sat_location_details (
  hkey_location     TEXT            PRIMARY KEY,    -- hash key: MD5([city,state,postal_code])
  country           VARCHAR(50)     NOT NULL,
  city              VARCHAR(50)     NOT NULL,
  state             VARCHAR(50)     NOT NULL,
  postal_code       VARCHAR(50)     NOT NULL,
  region            VARCHAR(50)     NOT NULL,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sat_transaction_details (
  hkey_transaction  TEXT            PRIMARY KEY,    -- hash key: MD5([city,state,postal_code,category,sub_category,segment,ship_mode,sales,quantity,discount])
  sales             DECIMAL(10,4)   NOT NULL    DEFAULT 0,
  quantity          INTEGER         NOT NULL    DEFAULT 0,
  discount          DECIMAL(5,4)    NOT NULL    DEFAULT 0,
  profit            DECIMAL(10,4)   NOT NULL    DEFAULT 0,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);


--
-- ЛИНКИ (Link) - связи между хабами
--

CREATE TABLE IF NOT EXISTS link_sales_transaction (
  hkey_sales_link   TEXT            PRIMARY KEY,
  hkey_transaction  TEXT            NOT NULL,
  hkey_ship_mode    TEXT            NOT NULL,
  hkey_segment      TEXT            NOT NULL,
  hkey_product      TEXT            NOT NULL,
  hkey_location     TEXT            NOT NULL,
  loaded_at         TIMESTAMPTZ     NOT NULL    DEFAULT CURRENT_TIMESTAMP
);
