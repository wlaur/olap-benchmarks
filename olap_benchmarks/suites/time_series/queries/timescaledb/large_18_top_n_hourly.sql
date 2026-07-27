select time_bucket(INTERVAL '1 hour', time) as hr, avg(value) as avg_value
from data_large
where metric_name = 'process_364'
group by time_bucket(INTERVAL '1 hour', time)
order by avg_value desc nulls last
limit 10
