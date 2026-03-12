select
    *
from
    data_large
where
    time >= '2023-01-01'
    and time < '2023-06-01'
order by
    time
