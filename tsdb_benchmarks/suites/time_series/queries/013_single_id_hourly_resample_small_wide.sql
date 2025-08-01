select
    date_trunc('hour', time) as time,
    avg(process_21) as value
from
    data_small_wide
group by
    date_trunc('hour', time)
order by
    time
limit
    100
