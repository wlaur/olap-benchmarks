select
    time,
    value,
    prev_value
from (
    select
        time,
        process_364 as value,
        lag(process_364) over (order by time) as prev_value
    from
        data_large
) t
where
    value is null
    and prev_value is not null
order by
    time
