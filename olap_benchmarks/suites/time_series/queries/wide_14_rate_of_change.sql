select
    time,
    process_545 as value,
    process_545 - lag(process_545) over (order by time) as delta
from
    data_wide
order by
    time
limit
    10000
