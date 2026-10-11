SELECT time, value FROM (SELECT time, CAST(value AS DOUBLE PRECISION) AS value FROM eav100 WHERE metric_name = 'metric_0001') s ORDER BY time DESC LIMIT 50
