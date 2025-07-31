select
    date_trunc('hour', time) as time,
    avg(value)
from
    data_large_eav
where
    id = 221
group by
    time
order by
    time
limit
    100
