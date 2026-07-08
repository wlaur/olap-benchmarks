select distinct time
from data_wide
where metric_name = 'binary_1'
order by time desc
limit 50
