select
    time,
    process_4 as value
from
    data_tall
where
    time > '2024-08-01'
    and time < '2024-08-08'
order by
    time
