select
    time,
    process_4 as value
from
    data_tall
where
    process_4 > 500
order by
    time
limit
    10000
