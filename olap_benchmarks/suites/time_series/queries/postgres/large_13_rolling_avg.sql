with series as (
    select
        times.time,
        metric.value
    from (
        select time
        from data_large
        where metric_name = 'binary_1'
    ) times
    left join data_large metric
        on metric.time = times.time
       and metric.metric_name = 'process_364'
)
select
    time,
    value,
    avg(value) over (order by time rows between 59 preceding and current row) as rolling_avg
from series
order by time
limit 10000
