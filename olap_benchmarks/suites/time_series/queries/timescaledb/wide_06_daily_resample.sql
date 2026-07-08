with buckets as (
    select time_bucket(INTERVAL '1 day', time) as d
    from data_wide
    where metric_name = 'binary_1'
    group by time_bucket(INTERVAL '1 day', time)
),
values_by_bucket as (
    select time_bucket(INTERVAL '1 day', time) as d, avg(value) as value
    from data_wide
    where metric_name = 'process_545'
    group by time_bucket(INTERVAL '1 day', time)
)
select
    buckets.d,
    values_by_bucket.value
from buckets
left join values_by_bucket on values_by_bucket.d = buckets.d
order by buckets.d
