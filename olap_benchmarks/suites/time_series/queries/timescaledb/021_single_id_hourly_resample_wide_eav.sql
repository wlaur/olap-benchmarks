select
    time_bucket(INTERVAL '1 hour', time) as time,
    avg(value) as value
from
    data_wide_eav
where
    id = 589
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    time
limit
    100
