select
    avg(process_545) as val,
    count(process_545) as cnt
from
    data_wide
where
    time > '2024-12-10'
    and time < '2024-12-15'
