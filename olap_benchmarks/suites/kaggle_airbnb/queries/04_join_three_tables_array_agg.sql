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
    array_agg(DISTINCT rd.reviewer_id) as reviewer_ids
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
