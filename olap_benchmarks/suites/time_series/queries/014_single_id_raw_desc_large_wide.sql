select
    time,
    process_259 as value
from
    data_large_wide
order by
    time desc
limit
    10000
