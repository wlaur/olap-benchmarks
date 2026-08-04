select
    date_trunc('hour', time) as hr,
    avg(cast(process_1 as double)) as value_1,
    avg(cast(cast(binary_1 as int) as double)) as value_2,
    avg(cast(ratio_1 as double)) as value_3,
    avg(cast(process_2 as double)) as value_4,
    avg(cast(process_3 as double)) as value_5,
    avg(cast(deviation_1 as double)) as value_6
from
    data_tall
group by
    date_trunc('hour', time)
order by
    hr
limit
    100
