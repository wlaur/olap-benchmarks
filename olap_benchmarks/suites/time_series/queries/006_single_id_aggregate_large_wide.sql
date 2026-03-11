select
    avg(process_545) as val,
    count(process_545) as cnt
from
    data_large_wide
where
    time > '2023-02-02'
    and time < '2024-06-01'
