select
    date_trunc('hour', time) as hour,
    avg(process_364) as value
from
    data_large
group by
    hour
order by
    hour
limit
    100
