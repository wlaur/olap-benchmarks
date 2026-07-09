select time,
    max(value) filter (where metric_name = 'process_667') as value_1,
    cast(max(value) filter (where metric_name = 'binary_22') as integer) as value_2,
    max(value) filter (where metric_name = 'ratio_12') as value_3,
    max(value) filter (where metric_name = 'process_259') as value_4,
    max(value) filter (where metric_name = 'process_242') as value_5,
    max(value) filter (where metric_name = 'deviation_39') as value_6
from data_large
where metric_name in ('process_667', 'binary_22', 'ratio_12', 'process_259', 'process_242', 'deviation_39')
group by time
order by time
limit 10000
