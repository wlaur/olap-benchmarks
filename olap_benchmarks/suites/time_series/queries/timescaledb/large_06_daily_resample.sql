select time_bucket(INTERVAL '1 day', time) as d, avg(value) as value
from data_large
where metric_name = 'process_364'
group by time_bucket(INTERVAL '1 day', time)
order by d
