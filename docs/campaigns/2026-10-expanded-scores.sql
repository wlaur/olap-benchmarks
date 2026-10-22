WITH
parameters AS (
    SELECT 0.01 AS smoothing_seconds, true AS include_doris
),
query_latencies AS (
    SELECT
        r.db,
        r.suite,
        r.suite_scale_factor AS sf,
        s.query_name,
        median(try_cast(json_extract(s.metadata, '$.duration_ms') AS DOUBLE))
            FILTER (WHERE s.status = 'completed' AND s.result_status = 'ok') / 1000 AS seconds
    FROM run AS r
    JOIN run_step AS s ON s.run_id = r.id
    CROSS JOIN parameters AS p
    WHERE r.operation = 'select'
        AND r.status = 'completed'
        AND r.benchmark_profile = 'resident-hot-v1'
        AND r.suite != 'kaggle_airbnb'
        AND s.step_type = 'query'
        AND s.iteration_role = 'measured'
        AND (p.include_doris OR r.db != 'doris')
    GROUP BY ALL
),
query_baselines AS (
    SELECT suite, sf, query_name, min(seconds) AS fastest, max(seconds) AS slowest
    FROM query_latencies
    GROUP BY ALL
),
suite_scores AS (
    SELECT
        q.db,
        q.suite,
        q.sf,
        count(q.seconds) AS measured_queries,
        exp(avg(ln(
            CASE WHEN q.seconds IS NOT NULL
                THEN (q.seconds + p.smoothing_seconds) / (b.fastest + p.smoothing_seconds)
                ELSE greatest(
                    10,
                    2 * (b.slowest + p.smoothing_seconds) / (b.fastest + p.smoothing_seconds)
                )
            END
        ))) AS score
    FROM query_latencies AS q
    JOIN query_baselines AS b USING (suite, sf, query_name)
    CROSS JOIN parameters AS p
    GROUP BY q.db, q.suite, q.sf
),
suite_worst AS (
    SELECT suite, sf, max(score) AS worst
    FROM suite_scores
    GROUP BY ALL
),
engines AS (
    SELECT DISTINCT db FROM suite_scores
)
SELECT
    e.db,
    round(exp(avg(ln(coalesce(s.score, w.worst)))), 6) AS overall_score,
    count(s.score) AS measured_suite_scale_cells,
    count(*) AS ranked_suite_scale_cells,
    sum(coalesce(s.measured_queries, 0)) AS measured_queries
FROM engines AS e
CROSS JOIN suite_worst AS w
LEFT JOIN suite_scores AS s ON s.db = e.db AND s.suite = w.suite AND s.sf = w.sf
GROUP BY e.db
ORDER BY overall_score, e.db;
