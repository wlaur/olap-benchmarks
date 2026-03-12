select
    date_trunc('hour', time) as hr,
    avg(process_4) as avg_value
from
    data_tall
group by
    hr
order by
    avg_value desc
limit
    10
