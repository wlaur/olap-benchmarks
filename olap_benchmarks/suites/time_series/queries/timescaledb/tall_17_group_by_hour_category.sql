select
    time_bucket(INTERVAL '1 hour', time) as hr,
    binary_1,
    avg(process_4) as value
from
    data_tall
group by
    time_bucket(INTERVAL '1 hour', time),
    binary_1
order by
    hr,
    binary_1
limit
    200
