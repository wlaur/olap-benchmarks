select
    date_trunc('hour', time) as hour,
    avg(process_545) as avg_value
from
    data_wide
group by
    date_trunc('hour', time)
order by
    avg_value desc
limit
    10
