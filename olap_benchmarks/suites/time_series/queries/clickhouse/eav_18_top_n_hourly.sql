select
    date_trunc('hour', time) as hour,
    avg(value) as avg_value
from
    data_wide_eav
where
    id = 484
group by
    hour
order by
    avg_value desc
limit
    10
