select
    time,
    value,
    prev_value
from (
    select
        time,
        process_4 as value,
        lag(process_4) over (order by time) as prev_value
    from
        data_tall
) t
where
    value is null
    and prev_value is not null
order by
    time
