select
    avg(value) as val,
    count(value) as cnt
from
    data_wide_eav
where
    id = 484
    and time > '2024-09-01'
    and time < '2024-12-15'
