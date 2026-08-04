select
    date_trunc('day', time) as d,
    avg(cast(process_364 as double)) as value
from
    data_large
group by
    date_trunc('day', time)
order by
    d
