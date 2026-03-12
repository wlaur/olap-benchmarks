select
    time,
    value,
    avg(value) over (order by time rows between 59 preceding and current row) as rolling_avg
from
    data_wide_eav
where
    id = 484
order by
    time
limit
    10000
