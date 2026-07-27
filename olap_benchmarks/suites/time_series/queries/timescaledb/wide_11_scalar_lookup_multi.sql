with times as (
    select time
    from data_wide
    where metric_name = 'binary_1'
      and time = '2024-12-15 12:30:00'
),
metrics as (
    select time, metric_name, value
    from data_wide
    where time = '2024-12-15 12:30:00'
      and metric_name in ('process_667', 'binary_22', 'ratio_12', 'process_259', 'process_242', 'deviation_39')
)
select
    times.time,
    max(metrics.value) filter (where metrics.metric_name = 'process_667') as value_1,
    cast(max(metrics.value) filter (where metrics.metric_name = 'binary_22') as integer) as value_2,
    max(metrics.value) filter (where metrics.metric_name = 'ratio_12') as value_3,
    max(metrics.value) filter (where metrics.metric_name = 'process_259') as value_4,
    max(metrics.value) filter (where metrics.metric_name = 'process_242') as value_5,
    max(metrics.value) filter (where metrics.metric_name = 'deviation_39') as value_6
from times
left join metrics on metrics.time = times.time
group by times.time
