with stats as (
    select avg(value::double precision) as avg_val, count(value) as cnt
    from data_large
    where metric_name = 'process_364'
)
select
    min(value) as min_val,
    max(value) as max_val,
    stats.avg_val,
    sqrt(sum((value::double precision - stats.avg_val) ^ 2) / (stats.cnt - 1)) as stddev_val,
    stats.cnt
from data_large
cross join stats
where metric_name = 'process_364'
group by stats.avg_val, stats.cnt
