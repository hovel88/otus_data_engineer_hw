SELECT
    toHour(pickup_datetime) AS hour,
    AVG(trip_distance) AS avg_distance,
    AVG(total_amount) AS avg_total
FROM default.trips
GROUP BY hour
ORDER BY hour;