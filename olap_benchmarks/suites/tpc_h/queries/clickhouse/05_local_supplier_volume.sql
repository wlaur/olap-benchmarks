-- Rewritten from the official comma-join formulation into an explicit,
-- hand-ordered join chain. ClickHouse's greedy join reordering turns the
-- comma form into a customer x supplier build keyed on the nation key, which
-- explodes quadratically and exceeds the server memory limit at SF >= 10.
-- Streaming lineitem against small build sides runs in well under a second.
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM lineitem
JOIN orders ON l_orderkey = o_orderkey
JOIN customer ON c_custkey = o_custkey
JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey
JOIN nation ON s_nationkey = n_nationkey
JOIN region ON n_regionkey = r_regionkey
WHERE r_name = 'ASIA'
    AND o_orderdate >= date '1994-01-01'
    AND o_orderdate < date '1994-01-01' + INTERVAL 1 YEAR
GROUP BY n_name
ORDER BY revenue DESC;
