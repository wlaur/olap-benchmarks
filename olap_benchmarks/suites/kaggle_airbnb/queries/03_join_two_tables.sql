-- added explicit table prefixes to the cols to disambiguate
SELECT
    listing_id,
    cl.date,
    cl.price,
    cl.minimum_nights,
    l.name,
    l.host_id,
    l.host_name,
    ld.source,
    ld.property_type,
    ld.room_type,
    ld.has_availability,
    ld.availability_30,
    ld.availability_60,
    ld.availability_90,
    ld.availability_365
FROM
    calendar as cl
    LEFT JOIN listings l on cl.listing_id = l.id
    LEFT JOIN listings_detailed ld on cl.listing_id = ld.id;
