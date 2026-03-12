select
    date_trunc('hour', time) as hr,
    avgIf(value, id = 892) as value_1,
    avgIf(value, id = 22) as value_2,
    avgIf(value, id = 87) as value_3,
    avgIf(value, id = 484) as value_4,
    avgIf(value, id = 467) as value_5,
    avgIf(value, id = 189) as value_6
from
    data_wide_eav
where
    id in (892, 22, 87, 484, 467, 189)
group by
    hr
order by
    hr
limit
    100
