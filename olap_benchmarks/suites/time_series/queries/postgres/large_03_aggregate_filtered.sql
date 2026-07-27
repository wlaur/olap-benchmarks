select avg(value) as val, count(value) as cnt
from data_large
where metric_name = 'process_364'
and time > '2020-01-01' and time < '2024-06-01'
