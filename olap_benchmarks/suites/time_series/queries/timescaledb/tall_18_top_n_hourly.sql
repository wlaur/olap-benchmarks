select
    time_bucket(INTERVAL '1 hour', time) as hour,
    avg(process_4) as avg_value
from
    data_tall
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    avg_value desc
limit
    10
