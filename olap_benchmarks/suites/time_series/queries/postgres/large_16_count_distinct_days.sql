select count(*) as distinct_days
from (
    select distinct date_trunc('day', time) as d
    from data_large
    where metric_name = 'process_364'
) t
