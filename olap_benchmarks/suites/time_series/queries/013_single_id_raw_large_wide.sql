select
    time,
    process_259 as value
from
    data_large
order by
    time
limit
    10000
