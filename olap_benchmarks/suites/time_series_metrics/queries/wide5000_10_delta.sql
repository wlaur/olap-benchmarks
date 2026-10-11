SELECT time, value, value - LAG(value) OVER (ORDER BY time) AS delta FROM (SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS value FROM wide5000) s ORDER BY time LIMIT 1000
