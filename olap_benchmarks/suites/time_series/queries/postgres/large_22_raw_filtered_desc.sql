select time, value
from data_large
where metric_name = 'process_364'
and time > '2023-08-01' and time < '2023-08-08'
order by time desc
