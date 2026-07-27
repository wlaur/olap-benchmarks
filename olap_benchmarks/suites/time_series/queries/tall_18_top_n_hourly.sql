select
    date_trunc('hour', time) as hr,
    avg(process_4) as avg_value
from
    data_tall
group by
    date_trunc('hour', time)
order by
    avg_value desc nulls last
limit
    10
