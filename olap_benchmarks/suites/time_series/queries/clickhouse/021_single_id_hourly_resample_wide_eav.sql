select
    date_trunc('hour', time) as time,
    avg(value) as value
from
    data_wide_eav
where
    id = 589
group by
    time
order by
    time
limit
    100
