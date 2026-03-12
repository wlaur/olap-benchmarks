select
    date_trunc('hour', time) as hr,
    binary_1,
    avg(process_4) as value
from
    data_tall
group by
    date_trunc('hour', time),
    binary_1
order by
    hr,
    binary_1
limit
    200
