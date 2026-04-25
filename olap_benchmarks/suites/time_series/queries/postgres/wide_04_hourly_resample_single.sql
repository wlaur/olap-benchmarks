select date_trunc('hour', time) as hr, avg(value) as value
from data_wide
where metric_name = 'process_545'
group by date_trunc('hour', time)
order by hr
limit 100
