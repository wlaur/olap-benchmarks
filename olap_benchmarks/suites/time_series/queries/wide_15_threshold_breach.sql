select
    time,
    process_545 as value
from
    data_wide
where
    process_545 > 500
order by
    time
limit
    10000
