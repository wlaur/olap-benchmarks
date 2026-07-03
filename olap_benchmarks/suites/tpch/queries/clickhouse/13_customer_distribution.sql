SELECT
    c_count,
    count(*) AS custdist
FROM (
    SELECT
        c_custkey,
        count(o_orderkey) AS c_count
    FROM customer
    LEFT OUTER JOIN orders ON (c_custkey = o_custkey)
        AND (o_comment NOT LIKE '%special%requests%')
    GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY
    custdist DESC,
    c_count DESC
-- join_use_nulls: non-matched LEFT JOIN rows must produce NULL (not 0) for
-- count(o_orderkey) to be correct; the official harness sets this via settings.json
SETTINGS join_use_nulls = 1;
