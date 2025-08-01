select
    date_trunc('hour', time) as time,
    avg(
        case
            when id = 68 then value
        end
    ) as value_1,
    avg(
        cast(
            case
                when id = 10 then value
            end as int
        )
    ) as value_2,
    avg(
        case
            when id = 32 then value
        end
    ) as value_3,
    avg(
        case
            when id = 25 then value
        end
    ) as value_4,
    avg(
        case
            when id = 53 then value
        end
    ) as value_5,
    avg(
        case
            when id = 65 then value
        end
    ) as value_6
from
    data_small_eav
where
    id in (68, 10, 32, 25, 53, 65)
group by
    date_trunc('hour', time)
order by
    time
limit
    100;
