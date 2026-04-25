select time, value
from data_wide
where metric_name = 'process_545'
and time > '2024-10-01' and time < '2024-10-08'
order by time
