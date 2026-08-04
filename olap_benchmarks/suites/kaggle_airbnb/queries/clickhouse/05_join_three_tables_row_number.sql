SELECT
    cl.listing_id,
    ROUND(
        MAX(
            CASE
                -- explicit precision and scale: a bare Decimal means Decimal(10, 0) here but
                -- DECIMAL(18, 3) on the other engines, so the same price hashed as 2231 or 2231.0
                WHEN cl.price != 'empty' THEN CAST(
                    REPLACE(REPLACE(cl.price, '$', ''), ',', '') AS Nullable(Decimal(18, 2))
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
    -- length() counts bytes on ClickHouse but characters on the other engines, so a comment with
    -- non-ASCII text measured 190 here against 188 elsewhere
    max(lengthUTF8(rd.comments)) as max_comments,
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
    ld.availability_365
-- listings.id and listings_detailed.id are unique, so one group per listing_id makes this a
-- total order. Without it the row order, and therefore the answer hash, was unspecified.
ORDER BY
    cl.listing_id;
