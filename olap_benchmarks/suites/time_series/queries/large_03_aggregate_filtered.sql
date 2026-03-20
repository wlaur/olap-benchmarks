select
    avg(process_364) as val,
    count(process_364) as cnt
from
    data_large
where
    time > '2020-01-01'
    and time < '2024-06-01'
