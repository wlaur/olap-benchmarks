select
    time_bucket(INTERVAL '1 hour', time) as hr,
    avg(process_364) as avg_value
from
    data_large
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    avg_value desc
limit
    10
