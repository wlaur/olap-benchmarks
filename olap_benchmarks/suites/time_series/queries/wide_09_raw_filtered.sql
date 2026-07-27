select
    time,
    process_545 as value
from
    data_wide
where
    time > '2024-12-10'
    and time < '2024-12-17'
order by
    time
