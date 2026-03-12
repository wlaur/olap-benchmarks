select
    time_bucket(INTERVAL '1 hour', time) as hour,
    avg(process_545) as value
from
    data_wide
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    hour
limit
    100
