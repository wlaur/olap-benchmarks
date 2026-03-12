select
    min(value) as min_val,
    max(value) as max_val,
    avg(value) as avg_val,
    sys.stddev_samp(value) as stddev_val,
    count(value) as cnt
from
    data_wide_eav
where
    id = 484
