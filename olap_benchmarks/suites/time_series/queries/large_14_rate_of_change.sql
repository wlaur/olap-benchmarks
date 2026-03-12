select
    time,
    process_364 as value,
    process_364 - lag(process_364) over (order by time) as delta
from
    data_large
order by
    time
limit
    10000
