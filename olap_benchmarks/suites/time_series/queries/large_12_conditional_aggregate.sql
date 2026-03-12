select
    avg(process_667) as value
from
    data_large
where
    abs(deviation_39) < 10
