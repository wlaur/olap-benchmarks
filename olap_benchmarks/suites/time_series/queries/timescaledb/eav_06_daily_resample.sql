select
    time_bucket(INTERVAL '1 day', time) as d,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    time_bucket(INTERVAL '1 day', time)
order by
    d
