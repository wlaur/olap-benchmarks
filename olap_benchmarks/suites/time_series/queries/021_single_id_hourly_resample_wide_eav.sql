select
    date_trunc('hour', time) as time,
    avg(value) as value
from
    data_wide_eav
where
    id = 589
group by
    date_trunc('hour', time)
order by
    time
limit
    100
