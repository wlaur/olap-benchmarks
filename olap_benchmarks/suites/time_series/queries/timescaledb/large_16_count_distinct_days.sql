select count(*) as distinct_days
from (
    select distinct time_bucket(INTERVAL '1 day', time) as d
    from data_large
    where metric_name = 'process_364'
) t
