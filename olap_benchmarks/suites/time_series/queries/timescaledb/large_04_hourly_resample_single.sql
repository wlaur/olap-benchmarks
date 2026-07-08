with buckets as (
    select time_bucket(INTERVAL '1 hour', time) as hr
    from data_large
    where metric_name = 'binary_1'
    group by time_bucket(INTERVAL '1 hour', time)
),
values_by_bucket as (
    select time_bucket(INTERVAL '1 hour', time) as hr, avg(value) as value
    from data_large
    where metric_name = 'process_364'
    group by time_bucket(INTERVAL '1 hour', time)
)
select
    buckets.hr,
    values_by_bucket.value
from buckets
left join values_by_bucket on values_by_bucket.hr = buckets.hr
order by buckets.hr
limit 100
