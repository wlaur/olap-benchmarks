with stats as (
    select avg(process_4::double precision) as avg_val, count(process_4) as cnt
    from data_tall
)
select
    min(process_4) as min_val,
    max(process_4) as max_val,
    stats.avg_val,
    sqrt(sum((process_4::double precision - stats.avg_val) ^ 2) / (stats.cnt - 1)) as stddev_val,
    stats.cnt
from data_tall
cross join stats
group by stats.avg_val, stats.cnt
