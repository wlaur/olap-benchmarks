select
    min(process_545) as min_val,
    max(process_545) as max_val,
    avg(process_545) as avg_val,
    stddevSamp(process_545) as stddev_val,
    count(process_545) as cnt
from
    data_wide
