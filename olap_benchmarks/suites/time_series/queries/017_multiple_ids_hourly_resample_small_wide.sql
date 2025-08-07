select
    date_trunc('hour', time) as time,
    avg(process_21) as value_1,
    avg(cast(binary_1 as int)) as value_2,
    avg(ratio_2) as value_3,
    avg(process_37) as value_4,
    avg(process_24) as value_5,
    avg(deviation_4) as value_6
from
    data_small_wide
group by
    date_trunc('hour', time)
order by
    time
limit
    100
