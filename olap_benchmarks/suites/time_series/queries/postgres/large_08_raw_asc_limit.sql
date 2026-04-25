select time, value
from data_large
where metric_name = 'process_364'
order by time
limit 10000
