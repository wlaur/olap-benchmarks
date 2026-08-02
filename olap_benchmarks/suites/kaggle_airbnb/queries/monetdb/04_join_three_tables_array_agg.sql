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
    -- MonetDB does not have an array type, this is an approximation.
    -- json.tojsonarray() is not used here: it silently ignores ORDER BY, so its element order is
    -- engine-defined and the result differs between runs. group_concat() does honour ORDER BY,
    -- and renders the ids as integers rather than json.tojsonarray()'s '.000000' floats.
    '[' || group_concat(DISTINCT reviewer_id, ',' ORDER BY reviewer_id) || ']' AS reviewer_ids
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
    ld.availability_365
ORDER BY
    -- listing_id uniquely identifies each group, so this is a total order
    cl.listing_id;
