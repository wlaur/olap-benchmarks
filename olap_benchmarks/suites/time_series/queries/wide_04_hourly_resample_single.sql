select
    date_trunc('hour', time) as hour,
    avg(process_545) as value
from
    data_wide
group by
    date_trunc('hour', time)
order by
    hour
limit
    100
