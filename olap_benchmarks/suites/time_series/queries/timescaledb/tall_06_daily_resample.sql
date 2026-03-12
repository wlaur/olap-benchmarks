select
    time_bucket(INTERVAL '1 day', time) as d,
    avg(process_4) as value
from
    data_tall
group by
    time_bucket(INTERVAL '1 day', time)
order by
    d
