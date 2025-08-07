select
    time,
    value
from
    data_large_eav
where
    id = 861
    and time > '2024-07-01'
    and time < '2024-07-15'
order by
    time
