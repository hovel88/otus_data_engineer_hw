SELECT
    toDayOfWeek(pickup_datetime) AS day_of_week,
    COUNT(*) AS trips_count,
    AVG(tip_amount) AS avg_tip
FROM default.trips
GROUP BY day_of_week
ORDER BY day_of_week;