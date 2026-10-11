SELECT date_trunc('hour', time) AS hr, AVG(value) AS value_1 FROM (SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS value FROM wide2000) s GROUP BY date_trunc('hour', time) ORDER BY hr
