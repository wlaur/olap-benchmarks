SELECT TIMETRUNCATE(time, 1h) AS hr,
       COUNT(*) AS samples,
       COUNT(score) AS valid_samples,
       AVG(score) AS avg_score,
       MIN(score) AS min_score,
       MAX(score) AS max_score,
       SUM(CASE WHEN score > 0.0 THEN 1 ELSE 0 END) AS positive_samples
FROM (
    SELECT time, CASE WHEN v10 IS NULL THEN CAST(NULL AS DOUBLE) WHEN v10 > 0.0 THEN 1.25 * ((v1 * (v2 - v3) + v4 * (v5 - v6) - ABS(v7 * v8)) / (1.0 + ABS(v9))) ELSE 0.75 * ((v1 * (v2 - v3) + v4 * (v5 - v6) - ABS(v7 * v8)) / (1.0 + ABS(v9))) END AS score
    FROM (
    SELECT time, MAX(CASE WHEN metric_name = 'metric_0001' THEN CAST(value AS DOUBLE) END) AS v1,
           MAX(CASE WHEN metric_name = 'metric_0455' THEN CAST(value AS DOUBLE) END) AS v2,
           MAX(CASE WHEN metric_name = 'metric_0910' THEN CAST(value AS DOUBLE) END) AS v3,
           MAX(CASE WHEN metric_name = 'metric_1365' THEN CAST(value AS DOUBLE) END) AS v4,
           MAX(CASE WHEN metric_name = 'metric_1820' THEN CAST(value AS DOUBLE) END) AS v5,
           MAX(CASE WHEN metric_name = 'metric_2274' THEN CAST(value AS DOUBLE) END) AS v6,
           MAX(CASE WHEN metric_name = 'metric_2729' THEN CAST(value AS DOUBLE) END) AS v7,
           MAX(CASE WHEN metric_name = 'metric_3184' THEN CAST(value AS DOUBLE) END) AS v8,
           MAX(CASE WHEN metric_name = 'metric_3639' THEN CAST(value AS DOUBLE) END) AS v9,
           MAX(CASE WHEN metric_name = 'metric_4094' THEN CAST(value AS DOUBLE) END) AS v10
    FROM eav4094
    WHERE time >= '2024-09-23' AND time < '2025-01-01'
      AND metric_name IN ('metric_0001', 'metric_0455', 'metric_0910', 'metric_1365', 'metric_1820', 'metric_2274', 'metric_2729', 'metric_3184', 'metric_3639', 'metric_4094')
    GROUP BY time
    ) aligned
) calculated
GROUP BY TIMETRUNCATE(time, 1h)
ORDER BY hr
