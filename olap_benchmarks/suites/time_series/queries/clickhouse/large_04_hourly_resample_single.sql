select
    date_trunc('hour', time) as hr,
    avg(process_364) as value
from
    data_large
group by
    hr
order by
    hr
limit
    100
