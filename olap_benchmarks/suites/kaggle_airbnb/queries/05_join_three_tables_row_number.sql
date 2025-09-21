SELECT
    cl.listing_id,
    ROUND(
        MAX(
            CASE
                WHEN cl.price != 'empty' THEN CAST(
                    REPLACE(REPLACE(cl.price, '$', ''), ',', '') AS DECIMAL
                )
                ELSE NULL
            END
        ),
        2
    ) AS max_price,
    max(cl.date) as max_date,
    max(cl.minimum_nights) as min_nights,
    max(cl.maximum_nights) as max_nights,
    ls.name,
    ls.host_name,
    ls.host_id,
    ld.property_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365,
    max(length(rd.comments)) as max_comments,
    row_number() over (
        PARTITION BY ls.host_id
        ORDER BY
            cl.listing_id ASC
    ) as row_host
FROM
    calendar as cl
    LEFT JOIN listings_detailed as ld ON cl.listing_id = ld.id
    LEFT JOIN listings as ls ON cl.listing_id = ls.id
    LEFT JOIN reviews_detailed rd on cl.listing_id = rd.listing_id
GROUP BY
    cl.listing_id,
    ls.name,
    ls.host_name,
    ld.property_type,
    ls.host_id,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365;
