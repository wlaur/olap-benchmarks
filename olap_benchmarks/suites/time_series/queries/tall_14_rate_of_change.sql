select
    time,
    process_4 as value,
    process_4 - lag(process_4) over (order by time) as delta
from
    data_tall
order by
    time
limit
    10000
