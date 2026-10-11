SELECT toStartOfHour(time) AS hr, AVG(value) AS value_1 FROM (SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS value FROM wide100) s GROUP BY toStartOfHour(time) ORDER BY hr
