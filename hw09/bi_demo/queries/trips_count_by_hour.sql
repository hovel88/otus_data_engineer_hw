SELECT
    toHour(pickup_datetime) AS hour,
    COUNT(*) AS trips_count
FROM default.trips
GROUP BY hour
ORDER BY hour;