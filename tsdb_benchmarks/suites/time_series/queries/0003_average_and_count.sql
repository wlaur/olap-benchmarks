select
    avg(value),
    count(value)
from
    data_large_eav
where
    id = 235
    and time > '2024-02-02'
    and time < '2025-01-01'
