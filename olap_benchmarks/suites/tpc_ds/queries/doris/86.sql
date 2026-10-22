WITH rollup_input AS
  (SELECT ws_net_paid,
          i_category,
          i_class
   FROM web_sales,
        date_dim d1,
        item
   WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     AND d1.d_date_sk = ws_sold_date_sk
     AND i_item_sk = ws_item_sk),
     rollup_output AS
  (SELECT sum(ws_net_paid) AS total_sum,
          i_category,
          i_class,
          grouping(i_category)+grouping(i_class) AS lochierarchy,
          CASE
              WHEN grouping(i_class) = 0 THEN i_category
          END AS parent_key
   FROM rollup_input
   GROUP BY GROUPING SETS ((i_category, i_class), (i_category))
   UNION ALL
   SELECT sum(ws_net_paid) AS total_sum,
          NULL AS i_category,
          NULL AS i_class,
          2 AS lochierarchy,
          NULL AS parent_key
   FROM rollup_input)
SELECT total_sum,
       i_category,
       i_class,
       lochierarchy,
       rank_within_parent
FROM
  (SELECT total_sum,
          i_category,
          i_class,
          lochierarchy,
          CASE
              WHEN lochierarchy = 0 THEN i_category
          END AS order_category,
          rank() OVER (PARTITION BY lochierarchy,
                                    parent_key
                       ORDER BY total_sum DESC) AS rank_within_parent
   FROM rollup_output) AS grouped
ORDER BY lochierarchy DESC NULLS FIRST,
         order_category NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;
