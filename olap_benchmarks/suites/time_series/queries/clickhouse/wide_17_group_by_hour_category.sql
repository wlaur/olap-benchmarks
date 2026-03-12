select
    date_trunc('hour', time) as hr,
    binary_22,
    avg(process_545) as value
from
    data_wide
group by
    hr,
    binary_22
order by
    hr,
    binary_22
limit
    200
