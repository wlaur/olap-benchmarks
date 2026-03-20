select
    time,
    process_364 as value
from
    data_large
order by
    time desc
limit
    10000
