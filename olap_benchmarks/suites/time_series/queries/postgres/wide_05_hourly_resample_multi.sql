select date_trunc('hour', time) as hr,
    avg(value) filter (where metric_name = 'process_667') as value_1,
    avg(value) filter (where metric_name = 'binary_22') as value_2,
    avg(value) filter (where metric_name = 'ratio_12') as value_3,
    avg(value) filter (where metric_name = 'process_259') as value_4,
    avg(value) filter (where metric_name = 'process_242') as value_5,
    avg(value) filter (where metric_name = 'deviation_39') as value_6
from data_wide
where metric_name in ('process_667', 'binary_22', 'ratio_12', 'process_259', 'process_242', 'deviation_39')
group by date_trunc('hour', time)
order by hr
limit 100
