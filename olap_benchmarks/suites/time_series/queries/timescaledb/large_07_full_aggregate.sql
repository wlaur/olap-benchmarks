select min(value) as min_val, max(value) as max_val,
       avg(value) as avg_val, stddev(value) as stddev_val,
       count(value) as cnt
from data_large
where metric_name = 'process_364'
