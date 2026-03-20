select
    date_trunc('day', time) as d,
    avg(process_545) as value
from
    data_wide
group by
    d
order by
    d
