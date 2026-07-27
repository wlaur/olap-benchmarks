with times as (
    select time
    from data_wide
    where metric_name = 'binary_1'
      and time = '2024-12-15 12:30:00'
)
select
    times.time,
    metric.value
from times
left join data_wide metric
    on metric.time = times.time
   and metric.metric_name = 'process_545'
