select
    time_bucket(INTERVAL '1 hour', time) as hour,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    hour
limit
    100
