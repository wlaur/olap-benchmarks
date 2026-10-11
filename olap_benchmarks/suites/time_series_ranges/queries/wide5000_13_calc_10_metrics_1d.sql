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
    SELECT time, CAST(metric_0001 AS DOUBLE PRECISION) AS v1,
           CAST(metric_0556 AS DOUBLE PRECISION) AS v2,
           CAST(metric_1111 AS DOUBLE PRECISION) AS v3,
           CAST(metric_1667 AS DOUBLE PRECISION) AS v4,
           CAST(metric_2222 AS DOUBLE PRECISION) AS v5,
           CAST(metric_2778 AS DOUBLE PRECISION) AS v6,
           CAST(metric_3333 AS DOUBLE PRECISION) AS v7,
           CAST(metric_3889 AS DOUBLE PRECISION) AS v8,
           CAST(metric_4444 AS DOUBLE PRECISION) AS v9,
           CAST(metric_5000 AS DOUBLE PRECISION) AS v10
    FROM wide5000
    WHERE time >= '2024-12-31' AND time < '2025-01-01'
    ) aligned
) calculated
GROUP BY date_trunc('hour', time)
ORDER BY hr
