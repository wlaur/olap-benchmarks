select
    date_trunc('day', time) as day,
    avg(process_364) as value
from
    data_large
group by
    day
order by
    day
