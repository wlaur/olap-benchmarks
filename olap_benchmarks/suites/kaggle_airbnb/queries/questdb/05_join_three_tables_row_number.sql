WITH calendar_summary AS (
    SELECT
        listing_id,
        round(max(CASE WHEN price != 'empty' THEN
            cast(replace(replace(price, '$', ''), ',', '') AS DECIMAL(18, 2))
        ELSE NULL END), 2) AS max_price,
        max(date) AS max_date,
        max(minimum_nights) AS min_nights,
        max(maximum_nights) AS max_nights
    FROM calendar
    GROUP BY listing_id
), review_summary AS (
    SELECT listing_id, max(CASE WHEN comments IS NULL THEN NULL ELSE length(comments) END) AS max_comments
    FROM reviews_detailed
    GROUP BY listing_id
), listing_summary AS (
SELECT
    cl.listing_id,
    max(cl.max_price) AS max_price,
    max(cl.max_date) AS max_date,
    max(cl.min_nights) AS min_nights,
    max(cl.max_nights) AS max_nights,
    ls.name,
    ls.host_name,
    ls.host_id,
    ld.property_type,
    CASE WHEN ld.id IS NULL THEN cast(NULL AS INT)
        ELSE cast(ld.has_availability AS INT) END AS has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365,
    max(rd.max_comments) AS max_comments
FROM calendar_summary cl
LEFT JOIN listings_detailed ld ON cl.listing_id = ld.id
LEFT JOIN listings ls ON cl.listing_id = ls.id
LEFT JOIN review_summary rd ON cl.listing_id = rd.listing_id
GROUP BY
    cl.listing_id,
    ld.id,
    ls.name,
    ls.host_name,
    ls.host_id,
    ld.property_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365
)
SELECT *, row_number() OVER (PARTITION BY host_id ORDER BY listing_id) AS row_host
FROM listing_summary
ORDER BY listing_id;
