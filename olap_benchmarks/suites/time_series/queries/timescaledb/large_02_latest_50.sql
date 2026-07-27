select distinct time
from data_large
where metric_name = 'binary_1'
order by time desc
limit 50
