with times as (
    select time
    from data_wide
    where metric_name = 'binary_1'
      and time > '2024-12-10'
      and time < '2024-12-17'
)
select
    times.time,
    metric.value
from times
left join data_wide metric
    on metric.time = times.time
   and metric.metric_name = 'process_545'
order by times.time
