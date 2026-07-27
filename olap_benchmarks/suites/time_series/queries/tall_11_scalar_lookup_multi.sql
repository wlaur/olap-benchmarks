select
    time,
    process_1 as value_1,
    cast(binary_1 as int) as value_2,
    ratio_1 as value_3,
    process_2 as value_4,
    process_3 as value_5,
    deviation_1 as value_6
from
    data_tall
where
    time = '2024-10-15 12:30:00'
