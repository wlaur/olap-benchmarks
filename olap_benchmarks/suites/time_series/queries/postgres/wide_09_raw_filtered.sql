select time, value
from data_wide
where metric_name = 'process_545'
and time > '2024-12-10' and time < '2024-12-17'
order by time
