select
    time_bucket(INTERVAL '1 hour', time) as hr,
    avg(process_364) as value
from
    data_large
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    hr
limit
    100
