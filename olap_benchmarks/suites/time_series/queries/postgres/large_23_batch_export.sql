select time, metric_name, value
from data_large
where time >= '2024-01-01' and time < '2024-06-01'
order by time, metric_name
