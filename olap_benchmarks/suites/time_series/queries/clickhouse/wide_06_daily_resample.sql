select
    date_trunc('day', time) as day,
    avg(process_545) as value
from
    data_wide
group by
    day
order by
    day
