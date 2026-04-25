select distinct time
from data_wide
where metric_name = 'process_545'
order by time desc
limit 50
