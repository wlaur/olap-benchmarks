select
    date_trunc('hour', time) as hr,
    avg(process_4) as value
from
    data_tall
group by
    date_trunc('hour', time)
order by
    hr
limit
    100
