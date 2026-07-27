with times as (
    select time
    from data_large
    where metric_name = 'binary_1'
    order by time
    limit 10000
)
select
    times.time,
    metric.value
from times
left join data_large metric
    on metric.time = times.time
   and metric.metric_name = 'process_364'
order by times.time
