select time, value,
       value - lag(value) over (order by time) as delta
from data_large
where metric_name = 'process_364'
order by time
limit 10000
