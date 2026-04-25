with b as (
    select time, value as binary_value from data_large
    where metric_name = 'binary_22'
),
p as (
    select time, value from data_large
    where metric_name = 'process_364'
)
select date_trunc('hour', p.time) as hr,
       b.binary_value as binary_22,
       avg(p.value) as value
from p join b on p.time = b.time
group by hr, b.binary_value
order by hr, binary_22
limit 200
