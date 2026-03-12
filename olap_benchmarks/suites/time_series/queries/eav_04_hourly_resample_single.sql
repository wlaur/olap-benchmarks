select
    date_trunc('hour', time) as hour,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    date_trunc('hour', time)
order by
    hour
limit
    100
