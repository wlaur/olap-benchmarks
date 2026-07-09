with b as (
    select time, cast(value as integer) as binary_value from data_wide
    where metric_name = 'binary_22'
),
p as (
    select time, value from data_wide
    where metric_name = 'process_545'
)
select time_bucket(INTERVAL '1 hour', p.time) as hr,
       b.binary_value as binary_22,
       avg(p.value) as value
from p join b on p.time = b.time
group by hr, b.binary_value
order by hr, binary_22
limit 200
