select
    date_trunc('hour', time) as time,
    avg(process_667) as value_1,
    avg(cast(binary_22 as int)) as value_2,
    avg(ratio_12) as value_3,
    avg(process_259) as value_4,
    avg(process_242) as value_5,
    avg(deviation_39) as value_6
from
    data_large_wide
group by
    date_trunc('hour', time)
order by
    time
limit
    100
