select count(*) as distinct_days
from (
    select distinct time_bucket(INTERVAL '1 day', time) as d
    from data_wide
    where metric_name = 'binary_1'
) t
