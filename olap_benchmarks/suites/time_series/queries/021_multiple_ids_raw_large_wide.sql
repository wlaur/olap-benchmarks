select
    time,
    process_667 as value_1,
    binary_22 as value_2,
    ratio_12 as value_3,
    process_259 as value_4,
    process_242 as value_5,
    deviation_39 as value_6
from
    data_large_wide
order by
    time
limit
    10000
