select
    time_bucket(INTERVAL '1 hour', time) as hour,
    avg(value) filter (where id = 892) as value_1,
    avg(value) filter (where id = 22) as value_2,
    avg(value) filter (where id = 87) as value_3,
    avg(value) filter (where id = 484) as value_4,
    avg(value) filter (where id = 467) as value_5,
    avg(value) filter (where id = 189) as value_6
from
    data_wide_eav
where
    id in (892, 22, 87, 484, 467, 189)
group by
    time_bucket(INTERVAL '1 hour', time)
order by
    hour
limit
    100
