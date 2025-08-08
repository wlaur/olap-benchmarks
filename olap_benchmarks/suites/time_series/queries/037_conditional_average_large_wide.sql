select
    avg(process_667) as value
from
    data_large_wide
where
    abs(deviation_39) < 10
