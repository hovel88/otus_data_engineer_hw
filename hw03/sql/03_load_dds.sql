-- Функция для генерации хэша
CREATE OR REPLACE FUNCTION generate_hkey(text[])
RETURNS TEXT AS $$
    SELECT MD5(array_to_string($1, '||'))
$$ LANGUAGE SQL;


--
-- Загрузка данных в HUB и в SAT режимов доставки
--

INSERT INTO hub_ship_mode (hkey_ship_mode, bkey_ship_mode)
  SELECT
    generate_hkey(ARRAY[ship_mode]),
    ship_mode
  FROM stg_superstore
ON CONFLICT (bkey_ship_mode) DO NOTHING;

INSERT INTO sat_ship_mode_details (hkey_ship_mode, ship_mode)
  SELECT
    generate_hkey(ARRAY[ship_mode]),
    ship_mode
  FROM stg_superstore
ON CONFLICT (hkey_ship_mode) DO NOTHING;



--
-- Загрузка данных в HUB и в SAT сегментов клиентов
--

INSERT INTO hub_segment (hkey_segment, bkey_segment)
  SELECT
    generate_hkey(ARRAY[segment]),
    segment
  FROM stg_superstore
ON CONFLICT (bkey_segment) DO NOTHING;

INSERT INTO sat_segment_details (hkey_segment, segment)
  SELECT
    generate_hkey(ARRAY[segment]),
    segment
  FROM stg_superstore
ON CONFLICT (hkey_segment) DO NOTHING;



--
-- Загрузка данных в HUB и в SAT продуктов
--

INSERT INTO hub_product (hkey_product, bkey_product)
  SELECT
    generate_hkey(ARRAY[category, sub_category]),
    category || '|' || sub_category
  FROM stg_superstore
ON CONFLICT (bkey_product) DO NOTHING;

INSERT INTO sat_product_details (hkey_product, category, sub_category)
  SELECT
    generate_hkey(ARRAY[category, sub_category]),
    category, sub_category
  FROM stg_superstore
ON CONFLICT (hkey_product) DO NOTHING;



--
-- Загрузка данных в HUB и в SAT локаций
--

INSERT INTO hub_location (hkey_location, bkey_location)
  SELECT
    generate_hkey(ARRAY[city, state, postal_code]),
    city || '|' || state || '|' || postal_code
  FROM stg_superstore
ON CONFLICT (bkey_location) DO NOTHING;

INSERT INTO sat_location_details (hkey_location, country, city, state, postal_code, region)
  SELECT
    generate_hkey(ARRAY[city, state, postal_code]),
    country, city, state, postal_code, region
  FROM stg_superstore
ON CONFLICT (hkey_location) DO NOTHING;



--
-- Загрузка данных в HUB и в SAT транзакций деталей продаж
--

INSERT INTO hub_transaction (hkey_transaction, bkey_transaction)
  SELECT DISTINCT
    generate_hkey(ARRAY[city, state, postal_code, category, sub_category, segment, ship_mode, sales::text, quantity::text, discount::text]),
    city || '|' || state || '|' || postal_code || '|' || category || '|' || sub_category || '|' || segment || '|' || ship_mode || '|' || sales::text || '|' || quantity::text || '|' || discount::text
  FROM stg_superstore
ON CONFLICT (bkey_transaction) DO NOTHING;

INSERT INTO sat_transaction_details (hkey_transaction, sales, quantity, discount, profit)
  SELECT
    generate_hkey(ARRAY[city, state, postal_code, category, sub_category, segment, ship_mode, sales::text, quantity::text, discount::text]),
    sales, quantity, discount, profit
  FROM stg_superstore
ON CONFLICT (hkey_transaction) DO NOTHING;



--
-- Загрузка данных в LINK таблицу (связываем все сущности)
--
INSERT INTO link_sales_transaction (hkey_sales_link, hkey_transaction, hkey_ship_mode, hkey_segment, hkey_product, hkey_location)
  SELECT
    generate_hkey(ARRAY[t.bkey_transaction, m.bkey_ship_mode, s.bkey_segment, p.bkey_product, l.bkey_location]),
    t.hkey_transaction,
    m.hkey_ship_mode,
    s.hkey_segment,
    p.hkey_product,
    l.hkey_location
  FROM stg_superstore stg
  JOIN hub_ship_mode m    ON m.bkey_ship_mode   = stg.ship_mode
  JOIN hub_segment s      ON s.bkey_segment     = stg.segment
  JOIN hub_product p      ON p.bkey_product     = stg.category || '|' || stg.sub_category
  JOIN hub_location l     ON l.bkey_location    = stg.city || '|' || stg.state || '|' || stg.postal_code
  JOIN hub_transaction t  ON t.bkey_transaction = stg.city || '|' || stg.state || '|' || stg.postal_code || '|' || stg.category || '|' || stg.sub_category || '|' || stg.segment || '|' || stg.ship_mode || '|' || stg.sales::text || '|' || stg.quantity::text || '|' || stg.discount::text
ON CONFLICT (hkey_sales_link) DO NOTHING;
