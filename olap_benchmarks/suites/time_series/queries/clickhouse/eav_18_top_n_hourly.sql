select
    date_trunc('hour', time) as hr,
    avg(value) as avg_value
from
    data_wide_eav
where
    id = 484
group by
    hr
order by
    avg_value desc
limit
    10
