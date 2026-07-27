with times as (
    select time
    from data_large
    where metric_name = 'binary_1'
      and time = '2023-08-04 12:23:00'
)
select
    times.time,
    metric.value
from times
left join data_large metric
    on metric.time = times.time
   and metric.metric_name = 'process_364'
