select
    time,
    process_364 as value
from
    data_large
where
    process_364 > 500
order by
    time
limit
    10000
