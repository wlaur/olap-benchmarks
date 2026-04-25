select time, value
from data_wide
where metric_name = 'process_545' and value > 500
order by time
limit 10000
