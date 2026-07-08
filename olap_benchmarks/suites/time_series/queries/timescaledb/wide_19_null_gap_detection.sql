with series as (
    select
        times.time,
        metric.value
    from (
        select time
        from data_wide
        where metric_name = 'binary_1'
    ) times
    left join data_wide metric
        on metric.time = times.time
       and metric.metric_name = 'process_545'
)
select time, value, prev_value
from (
    select
        time,
        value,
        lag(value) over (order by time) as prev_value
    from series
) t
where value is null and prev_value is not null
order by time
