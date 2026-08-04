select
    time,
    process_364 as value,
    avg(cast(process_364 as double)) over (order by time rows between 59 preceding and current row) as rolling_avg
from
    data_large
order by
    time
limit
    10000
