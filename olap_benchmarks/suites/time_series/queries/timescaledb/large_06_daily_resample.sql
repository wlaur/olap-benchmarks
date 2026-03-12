select
    time_bucket(INTERVAL '1 day', time) as d,
    avg(process_364) as value
from
    data_large
group by
    time_bucket(INTERVAL '1 day', time)
order by
    d
