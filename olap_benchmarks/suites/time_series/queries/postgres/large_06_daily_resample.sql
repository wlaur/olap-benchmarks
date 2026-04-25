select date_trunc('day', time) as d, avg(value) as value
from data_large
where metric_name = 'process_364'
group by date_trunc('day', time)
order by d
