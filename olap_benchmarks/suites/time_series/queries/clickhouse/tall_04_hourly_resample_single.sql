select
    date_trunc('hour', time) as hr,
    avg(process_4) as value
from
    data_tall
group by
    hr
order by
    hr
limit
    100
