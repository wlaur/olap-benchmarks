select
    avg(process_545) as val,
    count(process_545) as cnt
from
    data_wide
where
    time > '2024-09-01'
    and time < '2024-12-15'
