WITH rollup_input AS
  (SELECT ss_net_profit,
          s_state,
          s_county
   FROM store_sales,
        date_dim d1,
        store
   WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     AND d1.d_date_sk = ss_sold_date_sk
     AND s_store_sk = ss_store_sk
     AND s_state IN
       (SELECT s_state
        FROM
          (SELECT s_state AS s_state,
                  rank() OVER (PARTITION BY s_state
                               ORDER BY sum(ss_net_profit) DESC) AS ranking
           FROM store_sales,
                store,
                date_dim
           WHERE d_month_seq BETWEEN 1200 AND 1200+11
             AND d_date_sk = ss_sold_date_sk
             AND s_store_sk = ss_store_sk
           GROUP BY s_state) tmp1
        WHERE ranking <= 5 )),
     rollup_output AS
  (SELECT sum(ss_net_profit) AS total_sum,
          s_state,
          s_county,
          grouping(s_state)+grouping(s_county) AS lochierarchy,
          CASE
              WHEN grouping(s_county) = 0 THEN s_state
          END AS parent_key
   FROM rollup_input
   GROUP BY GROUPING SETS ((s_state, s_county), (s_state))
   UNION ALL
   SELECT sum(ss_net_profit) AS total_sum,
          NULL AS s_state,
          NULL AS s_county,
          2 AS lochierarchy,
          NULL AS parent_key
   FROM rollup_input)
SELECT total_sum,
       s_state,
       s_county,
       lochierarchy,
       rank_within_parent
FROM
  (SELECT total_sum,
          s_state,
          s_county,
          lochierarchy,
          CASE
              WHEN lochierarchy = 0 THEN s_state
          END AS order_state,
          rank() OVER (PARTITION BY lochierarchy,
                                    parent_key
                       ORDER BY total_sum DESC) AS rank_within_parent
   FROM rollup_output) AS grouped
ORDER BY lochierarchy DESC,
         order_state,
         rank_within_parent
LIMIT 100;
