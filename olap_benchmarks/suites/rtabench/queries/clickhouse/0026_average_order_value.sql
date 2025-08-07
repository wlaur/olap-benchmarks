SELECT
  sum(oi.amount * p.price) / count(distinct o.order_id)
FROM
  orders o,
  order_items oi,
  products p
WHERE
  o.order_id = oi.order_id
  AND oi.product_id = p.product_id
  AND o.created_at >= '2024-01-01'
  AND o.created_at < '2024-01-07';
