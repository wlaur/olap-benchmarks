select
    date_trunc('hour', time) as hr,
    avg(process_545) as value
from
    data_wide
group by
    date_trunc('hour', time)
order by
    hr
limit
    100
