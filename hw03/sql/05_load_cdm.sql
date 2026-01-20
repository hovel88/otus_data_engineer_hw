--
-- Витрина продаж по регионам и категориям
--
INSERT INTO cdm_sales_by_region_category
  SELECT
    sl.region,
    sp.category,
    sp.sub_category,
    COUNT(DISTINCT lst.hkey_sales_link) as transaction_count,
    SUM(st.sales) as total_sales,
    SUM(st.profit) as total_profit,
    SUM(st.quantity) as total_quantity,
    CASE
        WHEN SUM(st.sales) > 0 THEN (SUM(st.profit) / SUM(st.sales)) * 100
        ELSE 0
    END as profit_to_sales_percent
  FROM link_sales_transaction lst
  JOIN sat_location_details sl      ON sl.hkey_location    = lst.hkey_location
  JOIN sat_product_details sp       ON sp.hkey_product     = lst.hkey_product
  JOIN sat_transaction_details st   ON st.hkey_transaction = lst.hkey_transaction
  GROUP BY sl.region, sp.category, sp.sub_category;



--
-- Витрина эффективности сегментов клиентов
--
INSERT INTO cdm_customer_segment_analysis
  SELECT
    ss.segment,
    sl.region,
    sm.ship_mode,
    COUNT(DISTINCT lst.hkey_sales_link) as transaction_count,
    SUM(st.sales) as total_sales,
    SUM(st.profit) as total_profit,
    AVG(st.discount) * 100 as avg_discount,
    AVG(st.sales) as avg_sales_per_transaction
  FROM link_sales_transaction lst
  JOIN sat_segment_details ss       ON ss.hkey_segment     = lst.hkey_segment
  JOIN sat_location_details sl      ON sl.hkey_location    = lst.hkey_location
  JOIN sat_ship_mode_details sm     ON sm.hkey_ship_mode   = lst.hkey_ship_mode
  JOIN sat_transaction_details st   ON st.hkey_transaction = lst.hkey_transaction
  GROUP BY ss.segment, sl.region, sm.ship_mode;
