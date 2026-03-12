select
    date_trunc('day', time) as d,
    avg(value) as value
from
    data_wide_eav
where
    id = 484
group by
    d
order by
    d
