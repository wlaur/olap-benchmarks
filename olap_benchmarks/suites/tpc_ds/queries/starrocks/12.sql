SELECT i_item_id,
       i_item_desc,
       i_category,
       i_class,
       i_current_price,
       sum(ws_ext_sales_price) AS itemrevenue,
       CAST(sum(ws_ext_sales_price)*100.0000 AS DOUBLE)/sum(sum(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio
FROM web_sales,
     item,
     date_dim
WHERE ws_item_sk = i_item_sk
  AND i_category IN ('Sports',
                     'Books',
                     'Home')
  AND ws_sold_date_sk = d_date_sk
  AND d_date BETWEEN cast('1999-02-22' AS date) AND cast('1999-03-24' AS date)
GROUP BY i_item_id,
         i_item_desc,
         i_category,
         i_class,
         i_current_price
ORDER BY i_category NULLS LAST,
         i_class NULLS LAST,
         i_item_id NULLS LAST,
         i_item_desc NULLS LAST,
         revenueratio NULLS LAST
LIMIT 100;
