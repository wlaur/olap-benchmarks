select time,
    max(value) filter (where metric_name = 'process_667') as value_1,
    max(value) filter (where metric_name = 'binary_22') as value_2,
    max(value) filter (where metric_name = 'ratio_12') as value_3,
    max(value) filter (where metric_name = 'process_259') as value_4,
    max(value) filter (where metric_name = 'process_242') as value_5,
    max(value) filter (where metric_name = 'deviation_39') as value_6
from data_wide
where time = '2024-12-15 12:30:00' and metric_name in ('process_667', 'binary_22', 'ratio_12', 'process_259', 'process_242', 'deviation_39')
group by time
