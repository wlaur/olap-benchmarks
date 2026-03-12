select
    date_trunc('day', time) as day,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    date_trunc('day', time)
order by
    day
