select
    date_trunc('hour', time) as hour,
    avg(process_4) as value
from
    data_tall
group by
    hour
order by
    hour
limit
    100
