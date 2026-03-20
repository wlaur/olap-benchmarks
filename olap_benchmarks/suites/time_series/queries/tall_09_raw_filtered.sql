select
    time,
    process_4 as value
from
    data_tall
where
    time > '2023-06-01'
    and time < '2023-06-08'
order by
    time
