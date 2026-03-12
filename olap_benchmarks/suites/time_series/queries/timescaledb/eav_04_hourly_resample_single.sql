select
    time_bucket(INTERVAL '1 hour', time) as hr,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    hr
limit
    100
