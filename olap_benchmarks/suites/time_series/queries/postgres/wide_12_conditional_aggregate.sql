with valid_times as (
    select time from data_wide
    where metric_name = 'deviation_39' and abs(value) < 10
)
select avg(value) as value
from data_wide
where metric_name = 'process_667'
and time in (select time from valid_times)
