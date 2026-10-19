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
    SELECT listing_id, max(length(comments)) AS max_comments
    FROM reviews_detailed
    GROUP BY listing_id
)
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
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365,
    max(rd.max_comments) AS max_comments,
    row_number() OVER (PARTITION BY ls.host_id ORDER BY cl.listing_id) AS row_host
FROM calendar_summary cl
LEFT JOIN listings_detailed ld ON cl.listing_id = ld.id
LEFT JOIN listings ls ON cl.listing_id = ls.id
LEFT JOIN review_summary rd ON cl.listing_id = rd.listing_id
GROUP BY
    cl.listing_id,
    ls.name,
    ls.host_name,
    ls.host_id,
    ld.property_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365
ORDER BY cl.listing_id;
