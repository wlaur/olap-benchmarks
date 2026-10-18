WITH calendar_listings AS (
    SELECT DISTINCT listing_id FROM calendar
), review_counts AS (
    SELECT listing_id, count(DISTINCT id) AS review_count
    FROM reviews_detailed
    GROUP BY listing_id
), unique_reviewers AS (
    SELECT DISTINCT listing_id, reviewer_id FROM reviews_detailed
), ordered_reviewers AS (
    SELECT listing_id, reviewer_id FROM unique_reviewers
    ORDER BY listing_id, CASE WHEN reviewer_id IS NULL THEN 1 ELSE 0 END, reviewer_id
), review_arrays AS (
    SELECT
        listing_id,
        '[' || string_agg(CASE WHEN reviewer_id IS NULL THEN 'null'
            ELSE cast(reviewer_id AS VARCHAR) END, ',') || ']' AS reviewer_ids
    FROM ordered_reviewers
    GROUP BY listing_id
)
SELECT
    cl.listing_id,
    l.host_name,
    l.name,
    ld.source,
    ld.property_type,
    ld.room_type,
    CASE WHEN ld.id IS NULL THEN cast(NULL AS INT)
        ELSE cast(ld.has_availability AS INT) END AS has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365,
    coalesce(rc.review_count, 0) AS review_count,
    ra.reviewer_ids AS reviewer_ids
FROM calendar_listings cl
LEFT JOIN listings l ON cl.listing_id = l.id
LEFT JOIN listings_detailed ld ON cl.listing_id = ld.id
LEFT JOIN review_counts rc ON cl.listing_id = rc.listing_id
LEFT JOIN review_arrays ra ON cl.listing_id = ra.listing_id
GROUP BY
    cl.listing_id,
    ld.id,
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
    rc.review_count,
    ra.reviewer_ids
ORDER BY cl.listing_id;
