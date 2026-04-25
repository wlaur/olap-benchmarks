select time, value, prev_value
from (
    select time, value,
           lag(value) over (order by time) as prev_value
    from data_wide
    where metric_name = 'process_545'
) t
where value is null and prev_value is not null
order by time
