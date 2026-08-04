-- MonetDB returns an empty result set, with no error and no schema, when the statement-level
-- ORDER BY contains a CASE over grouping() from a rollup. The ordering key is therefore computed
-- in the subquery and sorted on outside, which leaves the projected columns unchanged.
SELECT total_sum,
       i_category,
       i_class,
       lochierarchy,
       rank_within_parent
FROM
  (SELECT sum(ws_net_paid) AS total_sum ,
          i_category ,
          i_class ,
          grouping(i_category)+grouping(i_class) AS lochierarchy ,
          CASE
              WHEN grouping(i_category)+grouping(i_class) = 0 THEN i_category
          END AS order_category ,
          rank() OVER ( PARTITION BY grouping(i_category)+grouping(i_class),
                                     CASE
                                         WHEN grouping(i_class) = 0 THEN i_category
                                     END
                       ORDER BY sum(ws_net_paid) DESC) AS rank_within_parent
   FROM web_sales ,
        date_dim d1 ,
        item
   WHERE d1.d_month_seq BETWEEN 1200 AND 1200+11
     AND d1.d_date_sk = ws_sold_date_sk
     AND i_item_sk = ws_item_sk
   GROUP BY rollup(i_category,i_class)) AS grouped
ORDER BY lochierarchy DESC NULLS FIRST,
         order_category NULLS FIRST,
         rank_within_parent NULLS FIRST
LIMIT 100;
