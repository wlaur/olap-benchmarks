select
    min(process_4) as min_val,
    max(process_4) as max_val,
    avg(process_4) as avg_val,
    stddev(process_4) as stddev_val,
    count(process_4) as cnt
from
    data_tall
