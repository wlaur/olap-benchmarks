select
    avg(process_64) as val,
    count(process_64) as cnt
from
    data_small_wide
where
    time > '2024-02-02'
    and time < '2024-12-16 12:00:00'
