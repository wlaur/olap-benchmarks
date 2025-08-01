select
    date_trunc('hour', time) as time,
    avg(value) as value
from
    data_small_eav
where
    id = 68
group by
    date_trunc('hour', time)
order by
    time
limit
    100
