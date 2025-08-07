select
    *
from
    data_large_eav
where
    -- only export 1 month (vs 6 for wide)
    time >= '2023-01-01'
    and time < '2023-02-01'
order by
    time,
    id
