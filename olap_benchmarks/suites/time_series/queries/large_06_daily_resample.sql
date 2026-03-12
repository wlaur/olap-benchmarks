select
    date_trunc('day', time) as day,
    avg(process_364) as value
from
    data_large
group by
    date_trunc('day', time)
order by
    day
