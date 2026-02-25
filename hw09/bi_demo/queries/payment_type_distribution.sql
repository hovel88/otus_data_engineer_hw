SELECT
    COUNT(*) AS cnt,
    payment_type
FROM default.trips
GROUP BY payment_type;