SELECT metric_name, COUNT(value) AS cnt, SUM(CAST(value AS DOUBLE PRECISION)) AS total FROM eav1000 GROUP BY metric_name ORDER BY metric_name
