select
    time,
    value
from
    data_wide_eav
where
    id = 484
order by
    time
limit
    10000
