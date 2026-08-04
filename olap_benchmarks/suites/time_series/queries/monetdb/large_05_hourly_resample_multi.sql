select
    date_trunc('hour', time) as hr,
    avg(cast(process_667 as double)) as value_1,
    avg(cast(cast(binary_22 as int) as double)) as value_2,
    avg(cast(ratio_12 as double)) as value_3,
    avg(cast(process_259 as double)) as value_4,
    avg(cast(process_242 as double)) as value_5,
    avg(cast(deviation_39 as double)) as value_6
from
    data_large
group by
    date_trunc('hour', time)
order by
    hr
limit
    100
