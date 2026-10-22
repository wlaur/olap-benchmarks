WITH rollup_input AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          coalesce(ss_sales_price*ss_quantity,0) AS sumsales
   FROM store_sales,
        date_dim,
        store,
        item
   WHERE ss_sold_date_sk=d_date_sk
     AND ss_item_sk=i_item_sk
     AND ss_store_sk = s_store_sk
     AND d_month_seq BETWEEN 1200 AND 1200+11),
     rollup_output AS
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sum(sumsales) sumsales
   FROM rollup_input
   GROUP BY GROUPING SETS (
          (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id),
          (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy),
          (i_category, i_class, i_brand, i_product_name, d_year, d_qoy),
          (i_category, i_class, i_brand, i_product_name, d_year),
          (i_category, i_class, i_brand, i_product_name),
          (i_category, i_class, i_brand),
          (i_category, i_class),
          (i_category))
   UNION ALL
   SELECT NULL AS i_category,
          NULL AS i_class,
          NULL AS i_brand,
          NULL AS i_product_name,
          NULL AS d_year,
          NULL AS d_qoy,
          NULL AS d_moy,
          NULL AS s_store_id,
          sum(sumsales) sumsales
   FROM rollup_input)
SELECT *
FROM
  (SELECT i_category,
          i_class,
          i_brand,
          i_product_name,
          d_year,
          d_qoy,
          d_moy,
          s_store_id,
          sumsales,
          rank() OVER (PARTITION BY i_category
                       ORDER BY sumsales DESC) rk
   FROM
     rollup_output dw1) dw2
WHERE rk <= 100
ORDER BY i_category NULLS FIRST,
         i_class NULLS FIRST,
         i_brand NULLS FIRST,
         i_product_name NULLS FIRST,
         d_year NULLS FIRST,
         d_qoy NULLS FIRST,
         d_moy NULLS FIRST,
         s_store_id NULLS FIRST,
         sumsales NULLS FIRST,
         rk NULLS FIRST
LIMIT 100;
