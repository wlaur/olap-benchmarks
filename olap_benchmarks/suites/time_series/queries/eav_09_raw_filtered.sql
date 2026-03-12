select
    time,
    value
from
    data_wide_eav
where
    id = 484
    and time > '2024-10-01'
    and time < '2024-10-08'
order by
    time
