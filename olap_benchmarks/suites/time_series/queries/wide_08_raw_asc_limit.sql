select
    time,
    process_545 as value
from
    data_wide
order by
    time
limit
    10000
