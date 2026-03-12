select
    date_trunc('hour', time) as hr,
    binary_22,
    avg(process_364) as value
from
    data_large
group by
    hr,
    binary_22
order by
    hr,
    binary_22
limit
    200
