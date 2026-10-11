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
    SELECT time, CAST(metric_0001 AS DOUBLE) AS v1,
           CAST(metric_0455 AS DOUBLE) AS v2,
           CAST(metric_0910 AS DOUBLE) AS v3,
           CAST(metric_1365 AS DOUBLE) AS v4,
           CAST(metric_1820 AS DOUBLE) AS v5,
           CAST(metric_2274 AS DOUBLE) AS v6,
           CAST(metric_2729 AS DOUBLE) AS v7,
           CAST(metric_3184 AS DOUBLE) AS v8,
           CAST(metric_3639 AS DOUBLE) AS v9,
           CAST(metric_4094 AS DOUBLE) AS v10
    FROM wide4094
    WHERE time >= '2024-09-23' AND time < '2025-01-01'
    ) aligned
) calculated
GROUP BY TIMETRUNCATE(time, 1h)
ORDER BY hr
