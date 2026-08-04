select
    time,
    process_545 as value,
    avg(cast(process_545 as double)) over (order by time rows between 59 preceding and current row) as rolling_avg
from
    data_wide
order by
    time
limit
    10000
