select
    time,
    process_4 as value,
    avg(cast(process_4 as double)) over (order by time rows between 59 preceding and current row) as rolling_avg
from
    data_tall
order by
    time
limit
    10000
