select
    date_trunc('hour', time) as hour,
    binary_22,
    avg(process_364) as value
from
    data_large
group by
    hour,
    binary_22
order by
    hour,
    binary_22
limit
    200
