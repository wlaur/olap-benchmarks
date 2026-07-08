with times as (
    select time
    from data_wide
    where metric_name = 'binary_1'
    order by time
    limit 10000
)
select
    times.time,
    metric.value
from times
left join data_wide metric
    on metric.time = times.time
   and metric.metric_name = 'process_545'
order by times.time
