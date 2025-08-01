select
    avg(value) as val,
    count(value) as cnt
from
    data_large_eav
where
    id = 991 -- column "process_64" in the data_small_wide table
    and time > '2023-02-02'
    and time < '2024-06-01'
