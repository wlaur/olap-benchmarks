-- CAGG_TOP_SELLING_PRODUCT_SEMESTER
CREATE MATERIALIZED VIEW cagg_top_selling_product_semester WITH (timescaledb.continuous) AS
SELECT
	time_bucket('6 month', event_created) AS event_created,
    product_id,
    p.name AS product_name,
    sum(oi.amount * p.price) AS volume,
    event_payload->>'terminal' as terminal
FROM
    products p
    INNER JOIN order_items oi USING (product_id)
    INNER JOIN order_events oe ON oe.order_id = oi.order_id
GROUP BY
    1, terminal, product_id, product_name
;

CREATE INDEX ON cagg_top_selling_product_semester(terminal,event_created,volume desc);
