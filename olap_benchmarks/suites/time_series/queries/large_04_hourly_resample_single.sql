select
    date_trunc('hour', time) as hour,
    avg(process_364) as value
from
    data_large
group by
    date_trunc('hour', time)
order by
    hour
limit
    100
