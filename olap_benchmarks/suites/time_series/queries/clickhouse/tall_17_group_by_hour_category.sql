select
    date_trunc('hour', time) as hour,
    binary_1,
    avg(process_4) as value
from
    data_tall
group by
    hour,
    binary_1
order by
    hour,
    binary_1
limit
    200
