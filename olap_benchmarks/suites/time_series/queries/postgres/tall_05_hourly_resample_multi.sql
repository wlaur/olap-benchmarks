select
    date_trunc('hour', time) as hr,
    avg(process_1) as value_1,
    avg(cast(cast(binary_1 as int) as double precision)) as value_2,
    avg(ratio_1) as value_3,
    avg(process_2) as value_4,
    avg(process_3) as value_5,
    avg(deviation_1) as value_6
from
    data_tall
group by
    date_trunc('hour', time)
order by
    hr
limit
    100
