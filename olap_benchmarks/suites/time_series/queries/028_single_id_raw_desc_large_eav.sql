select
    time,
    value
from
    data_large_eav
where
    id = 861
order by
    time desc
limit
    10000
