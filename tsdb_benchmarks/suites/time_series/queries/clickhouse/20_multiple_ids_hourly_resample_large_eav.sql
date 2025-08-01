select
    date_trunc('hour', time) as time,
    avg(
        case
            when id = 371 then value
        end
    ) as value_1,
    avg(
        cast(
            case
                when id = 364 then value
            end as Nullable(int)
        )
    ) as value_2,
    avg(
        case
            when id = 407 then value
        end
    ) as value_3,
    avg(
        case
            when id = 861 then value
        end
    ) as value_4,
    avg(
        case
            when id = 984 then value
        end
    ) as value_5,
    avg(
        case
            when id = 830 then value
        end
    ) as value_6
from
    data_large_eav
where
    id in (371, 364, 407, 861, 984, 830)
group by
    time
order by
    time
limit
    100;
