select
    date_trunc('hour', time) as time,
    avg(value) as value
from
    data_large_eav
where
    id = 816
group by
    time
order by
    time
limit
    100
