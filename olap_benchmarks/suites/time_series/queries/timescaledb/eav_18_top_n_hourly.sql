select
    time_bucket(INTERVAL '1 hour', time) as hour,
    avg(value) as avg_value
from
    data_wide_eav
where
    id = 484
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    avg_value desc
limit
    10
