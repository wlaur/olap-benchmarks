select
    time,
    process_667 as value_1,
    cast(binary_22 as int) as value_2,
    ratio_12 as value_3,
    process_259 as value_4,
    process_242 as value_5,
    deviation_39 as value_6
from
    data_large_wide
where
    time = '2023-11-24 06:23'
