select
    date_trunc('hour', time) as hr,
    avg(process_364) as avg_value
from
    data_large
group by
    date_trunc('hour', time)
order by
    avg_value desc
limit
    10
