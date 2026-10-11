SELECT COUNT(COLUMNS('metric_.*')) AS "cnt_\0", SUM(CAST(COLUMNS('metric_.*') AS DOUBLE)) AS "total_\0" FROM wide1000
