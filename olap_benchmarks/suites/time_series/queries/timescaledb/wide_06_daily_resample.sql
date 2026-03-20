select
    time_bucket(INTERVAL '1 day', time) as d,
    avg(process_545) as value
from
    data_wide
group by
    time_bucket(INTERVAL '1 day', time)
order by
    d
