select
    time,
    process_259 as value
from
    data_large_wide
where
    time > '2024-07-01'
    and time < '2024-07-15'
order by
    time desc
