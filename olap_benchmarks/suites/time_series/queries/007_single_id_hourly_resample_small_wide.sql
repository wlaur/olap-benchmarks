select
    date_trunc('hour', time) as time,
    avg(process_4) as value
from
    data_tall
group by
    date_trunc('hour', time)
order by
    time
limit
    100
