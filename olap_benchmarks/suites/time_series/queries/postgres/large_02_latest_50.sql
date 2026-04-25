select distinct time
from data_large
where metric_name = 'process_364'
order by time desc
limit 50
