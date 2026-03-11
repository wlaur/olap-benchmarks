select
    date_trunc('hour', time) as time,
    avg(process_364) as value
from
    data_large_wide
group by
    date_trunc('hour', time)
order by
    time
limit
    100
