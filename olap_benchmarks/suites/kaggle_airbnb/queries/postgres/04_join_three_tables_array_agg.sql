WITH calendar_listings AS (
    SELECT DISTINCT listing_id FROM calendar
), review_summary AS (
    SELECT
        listing_id,
        count(DISTINCT id) AS review_count,
        array_agg(DISTINCT reviewer_id ORDER BY reviewer_id) AS reviewer_ids
    FROM reviews_detailed
    GROUP BY listing_id
)
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
    coalesce(rd.review_count, 0) AS review_count,
    coalesce(rd.reviewer_ids, ARRAY[NULL::BIGINT]) AS reviewer_ids
FROM calendar_listings cl
LEFT JOIN listings l ON cl.listing_id = l.id
LEFT JOIN listings_detailed ld ON cl.listing_id = ld.id
LEFT JOIN review_summary rd ON cl.listing_id = rd.listing_id
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
    ld.availability_365,
    rd.review_count,
    rd.reviewer_ids
ORDER BY cl.listing_id;
