select date_trunc('hour', time) as hr, avg(value) as avg_value
from data_large
where metric_name = 'process_364'
group by date_trunc('hour', time)
order by avg_value desc
limit 10
