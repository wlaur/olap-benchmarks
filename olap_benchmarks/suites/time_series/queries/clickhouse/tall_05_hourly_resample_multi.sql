select
    date_trunc('hour', time) as hour,
    avg(process_1) as value_1,
    avg(cast(binary_1 as Nullable(int))) as value_2,
    avg(ratio_1) as value_3,
    avg(process_2) as value_4,
    avg(process_3) as value_5,
    avg(deviation_1) as value_6
from
    data_tall
group by
    hour
order by
    hour
limit
    100
