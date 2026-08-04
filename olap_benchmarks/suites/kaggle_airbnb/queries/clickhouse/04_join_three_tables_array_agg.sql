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
    -- ClickHouse has no array_agg(DISTINCT ... ORDER BY ...); groupUniqArray() deduplicates but
    -- returns elements in an engine-defined order, so arraySort() supplies the deterministic
    -- element order the other engines get from the ORDER BY inside the aggregate.
    -- groupUniqArray() also drops NULLs, so a listing with no reviews yields [] where array_agg()
    -- yields [NULL] from the unmatched LEFT JOIN row; emit [NULL] to keep the two comparable.
    if(
        empty(groupUniqArray(rd.reviewer_id)),
        CAST([NULL], 'Array(Nullable(Int64))'),
        CAST(arraySort(groupUniqArray(rd.reviewer_id)), 'Array(Nullable(Int64))')
    ) as reviewer_ids
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
