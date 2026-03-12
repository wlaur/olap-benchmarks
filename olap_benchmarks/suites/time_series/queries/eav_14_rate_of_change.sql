select
    time,
    value,
    value - lag(value) over (order by time) as delta
from
    data_wide_eav
where
    id = 484
order by
    time
limit
    10000
