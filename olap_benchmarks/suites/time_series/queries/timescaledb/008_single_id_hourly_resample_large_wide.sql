select
    time_bucket(INTERVAL '1 hour', time) as time,
    avg(process_364) as value
from
    data_wide
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    time
limit
    100
