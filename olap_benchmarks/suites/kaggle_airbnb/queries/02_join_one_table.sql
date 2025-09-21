-- added explicit table prefixes to the cols to disambiguate
SELECT
    cl.listing_id,
    cl.date,
    cl.price,
    cl.minimum_nights,
    l.name,
    l.host_id,
    l.host_name
FROM
    calendar as cl
    LEFT JOIN listings l on cl.listing_id = l.id
ORDER BY
    cl.listing_id ASC;
