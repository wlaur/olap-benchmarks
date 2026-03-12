select
    date_trunc('hour', time) as hour,
    avg(process_364) as avg_value
from
    data_large
group by
    hour
order by
    avg_value desc
limit
    10
