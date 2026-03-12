select
    count(*) as distinct_days
from (
    select distinct date_trunc('day', time)
    from data_tall
) t
