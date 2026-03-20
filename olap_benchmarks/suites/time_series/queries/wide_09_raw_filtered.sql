select
    time,
    process_545 as value
from
    data_wide
where
    time > '2024-10-01'
    and time < '2024-10-08'
order by
    time
