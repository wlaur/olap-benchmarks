select
    date_trunc('hour', time) as hr,
    avg(process_545) as avg_value
from
    data_wide
group by
    hr
order by
    avg_value desc nulls last
limit
    10
