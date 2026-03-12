select
    date_trunc('hour', time) as hour,
    avg(process_4) as avg_value
from
    data_tall
group by
    date_trunc('hour', time)
order by
    avg_value desc
limit
    10
