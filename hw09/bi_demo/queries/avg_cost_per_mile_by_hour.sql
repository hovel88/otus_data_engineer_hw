SELECT
    toHour(pickup_datetime) AS hour,
    AVG(total_amount / trip_distance) AS avg_cost_per_mile,
    COUNT(*) AS trips_count
FROM default.trips
WHERE trip_distance > 0 AND trip_distance < 30
GROUP BY hour
ORDER BY hour;