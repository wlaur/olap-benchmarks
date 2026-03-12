select
    time,
    value
from
    data_wide_eav
where
    id = 484
    and time > '2024-11-01'
    and time < '2024-11-15'
order by
    time
