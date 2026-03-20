select
    time,
    process_364 as value
from
    data_large
order by
    time
limit
    10000
