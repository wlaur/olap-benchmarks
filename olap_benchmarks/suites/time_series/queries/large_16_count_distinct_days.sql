select
    count(distinct date_trunc('day', time)) as distinct_days
from
    data_large
