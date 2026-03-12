select
    date_trunc('hour', time) as time,
    avg(process_4) as value_1,
    avg(cast(binary_1 as int)) as value_2,
    avg(ratio_1) as value_3,
    avg(process_5) as value_4,
    avg(process_6) as value_5,
    avg(deviation_1) as value_6
from
    data_tall
group by
    date_trunc('hour', time)
order by
    time
limit
    100
