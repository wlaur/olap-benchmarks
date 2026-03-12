select
    date_trunc('hour', time) as hour,
    avg(process_545) as value
from
    data_wide
group by
    hour
order by
    hour
limit
    100
