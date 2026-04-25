select avg(value) as val, count(value) as cnt
from data_wide
where metric_name = 'process_545'
and time > '2024-09-01' and time < '2024-12-15'
