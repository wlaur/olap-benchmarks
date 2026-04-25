select date_trunc('hour', time) as hr, avg(value) as value
from data_large
where metric_name = 'process_364'
group by date_trunc('hour', time)
order by hr
limit 100
