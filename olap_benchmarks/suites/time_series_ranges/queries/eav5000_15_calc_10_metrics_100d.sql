SELECT date_trunc('hour', time) AS hr,
       COUNT(*) AS samples,
       COUNT(score) AS valid_samples,
       AVG(score) AS avg_score,
       MIN(score) AS min_score,
       MAX(score) AS max_score,
       SUM(CASE WHEN score > 0.0 THEN 1 ELSE 0 END) AS positive_samples
FROM (
    SELECT time, CASE WHEN v10 IS NULL THEN CAST(NULL AS DOUBLE PRECISION) WHEN v10 > 0.0 THEN 1.25 * ((v1 * (v2 - v3) + v4 * (v5 - v6) - ABS(v7 * v8)) / (1.0 + ABS(v9))) ELSE 0.75 * ((v1 * (v2 - v3) + v4 * (v5 - v6) - ABS(v7 * v8)) / (1.0 + ABS(v9))) END AS score
    FROM (
    SELECT time, MAX(CASE WHEN metric_name = 'metric_0001' THEN CAST(value AS DOUBLE PRECISION) END) AS v1,
           MAX(CASE WHEN metric_name = 'metric_0556' THEN CAST(value AS DOUBLE PRECISION) END) AS v2,
           MAX(CASE WHEN metric_name = 'metric_1111' THEN CAST(value AS DOUBLE PRECISION) END) AS v3,
           MAX(CASE WHEN metric_name = 'metric_1667' THEN CAST(value AS DOUBLE PRECISION) END) AS v4,
           MAX(CASE WHEN metric_name = 'metric_2222' THEN CAST(value AS DOUBLE PRECISION) END) AS v5,
           MAX(CASE WHEN metric_name = 'metric_2778' THEN CAST(value AS DOUBLE PRECISION) END) AS v6,
           MAX(CASE WHEN metric_name = 'metric_3333' THEN CAST(value AS DOUBLE PRECISION) END) AS v7,
           MAX(CASE WHEN metric_name = 'metric_3889' THEN CAST(value AS DOUBLE PRECISION) END) AS v8,
           MAX(CASE WHEN metric_name = 'metric_4444' THEN CAST(value AS DOUBLE PRECISION) END) AS v9,
           MAX(CASE WHEN metric_name = 'metric_5000' THEN CAST(value AS DOUBLE PRECISION) END) AS v10
    FROM eav5000
    WHERE time >= '2024-09-23' AND time < '2025-01-01'
      AND metric_name IN ('metric_0001', 'metric_0556', 'metric_1111', 'metric_1667', 'metric_2222', 'metric_2778', 'metric_3333', 'metric_3889', 'metric_4444', 'metric_5000')
    GROUP BY time
    ) aligned
) calculated
GROUP BY date_trunc('hour', time)
ORDER BY hr
