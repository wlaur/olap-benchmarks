select
    time,
    process_364 as value
from
    data_large
where
    time > '2023-08-01'
    and time < '2023-08-08'
order by
    time
