select time_bucket(INTERVAL '1 hour', time) as hr, avg(value) as avg_value
from data_wide
where metric_name = 'process_545'
group by time_bucket(INTERVAL '1 hour', time)
order by avg_value desc
limit 10
