select date_trunc('day', time) as d, avg(value) as value
from data_wide
where metric_name = 'process_545'
group by date_trunc('day', time)
order by d
