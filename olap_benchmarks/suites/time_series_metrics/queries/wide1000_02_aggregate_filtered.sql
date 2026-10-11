SELECT AVG(value) AS value, COUNT(value) AS cnt FROM (SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS value FROM wide1000) s WHERE time >= '2024-12-29' AND time < '2024-12-30'
