SELECT
    toStartOfMonth(pickup_datetime) AS month,
    AVG(fare_amount) AS avg_fare,
    AVG(extra) AS avg_extra,
    AVG(tip_amount) AS avg_tip,
    AVG(tolls_amount) AS avg_tolls
FROM default.trips
GROUP BY month
ORDER BY month;