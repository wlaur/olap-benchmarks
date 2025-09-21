-- TODO: this query is very expensive also with LIMIT 1, fetching with method binary makes this 2x slower
SELECT
    cl.listing_id,
    l.host_name,
    l.name,
    ld.source,
    ld.property_type,
    ld.room_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365,
    count(DISTINCT rd.id) as review_count,
    -- MonetDB does not have an array type, this is an approximation
    -- renders JSON floats (with .0000 decimal), this is not strictly correct
    json.tojsonarray(DISTINCT reviewer_id) AS reviewer_ids
FROM
    calendar as cl
    LEFT JOIN listings l on cl.listing_id = l.id
    LEFT JOIN listings_detailed ld on cl.listing_id = ld.id
    LEFT JOIN reviews_detailed rd on cl.listing_id = rd.listing_id
GROUP BY
    cl.listing_id,
    l.host_name,
    l.name,
    ld.source,
    ld.property_type,
    ld.room_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365;
