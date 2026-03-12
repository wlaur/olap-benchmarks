select
    time,
    process_259 as value
from
    data_large
order by
    time desc
limit
    10000
