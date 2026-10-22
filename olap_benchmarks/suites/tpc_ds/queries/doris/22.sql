WITH rollup_input AS
  (SELECT i_product_name,
          i_brand,
          i_class,
          i_category,
          inv_quantity_on_hand AS qoh
   FROM inventory ,
        date_dim ,
        item
   WHERE inv_date_sk=d_date_sk
     AND inv_item_sk=i_item_sk
     AND d_month_seq BETWEEN 1200 AND 1200 + 11),
     rollup_output AS
  (SELECT i_product_name,
          i_brand,
          i_class,
          i_category,
          avg(qoh) qoh
   FROM rollup_input
   GROUP BY GROUPING SETS (
          (i_product_name, i_brand, i_class, i_category),
          (i_product_name, i_brand, i_class),
          (i_product_name, i_brand),
          (i_product_name))
   UNION ALL
   SELECT NULL AS i_product_name,
          NULL AS i_brand,
          NULL AS i_class,
          NULL AS i_category,
          avg(qoh) qoh
   FROM rollup_input)
SELECT *
FROM rollup_output
ORDER BY qoh NULLS FIRST,
         i_product_name NULLS FIRST,
         i_brand NULLS FIRST,
         i_class NULLS FIRST,
         i_category NULLS FIRST
LIMIT 100;
