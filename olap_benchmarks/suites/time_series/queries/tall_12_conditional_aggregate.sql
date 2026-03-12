select
    avg(process_5) as value
from
    data_tall
where
    abs(deviation_1) < 10
