select
    date_trunc('hour', time) as time,
    avg(case when id = 892 then value end) as value_1,
    avg(case when id = 22 then value end) as value_2,
    avg(case when id = 87 then value end) as value_3,
    avg(case when id = 484 then value end) as value_4,
    avg(case when id = 467 then value end) as value_5,
    avg(case when id = 189 then value end) as value_6
from
    data_wide_eav
where
    id in (892, 22, 87, 484, 467, 189)
group by
    date_trunc('hour', time)
order by
    time
limit
    100
