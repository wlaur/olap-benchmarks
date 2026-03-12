select
    time,
    process_4 as value
from
    data_tall
order by
    time
limit
    10000
