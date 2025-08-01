select
    avg(value) as val,
    count(value) as cnt
from
    data_small_eav
where
    id = 5 -- column "process_64" in the data_small_wide table
    and time > '2024-02-02'
    and time < '2024-12-16 12:00:00'
