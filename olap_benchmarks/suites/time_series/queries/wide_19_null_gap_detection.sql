select
    time,
    value,
    prev_value
from (
    select
        time,
        process_545 as value,
        lag(process_545) over (order by time) as prev_value
    from
        data_wide
) t
where
    value is null
    and prev_value is not null
order by
    time
