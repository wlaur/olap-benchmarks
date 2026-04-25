select max(time)
from data_wide
where metric_name = 'process_545'
