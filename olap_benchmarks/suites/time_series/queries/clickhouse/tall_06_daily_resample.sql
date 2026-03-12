select
    date_trunc('day', time) as d,
    avg(process_4) as value
from
    data_tall
group by
    d
order by
    d
