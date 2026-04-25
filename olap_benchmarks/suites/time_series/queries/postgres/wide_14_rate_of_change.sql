select time, value,
       value - lag(value) over (order by time) as delta
from data_wide
where metric_name = 'process_545'
order by time
limit 10000
