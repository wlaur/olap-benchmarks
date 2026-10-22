-- Rewritten from the official view-based formulation (CREATE VIEW revenue0 /
-- SELECT / DROP VIEW) to a single CTE statement, because the benchmark runner
-- executes one statement per query.
WITH revenue0 AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM lineitem
    WHERE (l_shipdate >= date '1996-01-01')
        AND (l_shipdate < date '1996-01-01' + INTERVAL 3 MONTH)
    GROUP BY l_suppkey
), revenue_with_max AS (
    SELECT
        supplier_no,
        total_revenue,
        max(total_revenue) OVER () AS max_revenue
    FROM revenue0
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
FROM supplier, revenue_with_max
WHERE (s_suppkey = supplier_no)
    AND (total_revenue = max_revenue)
ORDER BY s_suppkey;
