SELECT toStartOfHour(time) AS hr, AVG(value) AS value_1 FROM (SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS value FROM wide5000) s GROUP BY toStartOfHour(time) ORDER BY hr
