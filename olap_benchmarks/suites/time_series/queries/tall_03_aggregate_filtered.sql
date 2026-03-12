select
    avg(process_4) as val,
    count(process_4) as cnt
from
    data_tall
where
    time > '2022-06-01'
    and time < '2024-06-01'
