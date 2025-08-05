select
    time,
    avg(
        case
            when cast(id as int) = 371 then value
            else null
        end
    ) as value_1,
    cast(
        avg(
            cast(
                case
                    when cast(id as int) = 364 then value
                    else null
                end as int
            )
        ) as int
    ) as value_2,
    avg(
        case
            when cast(id as int) = 407 then value
            else null
        end
    ) as value_3,
    avg(
        case
            when cast(id as int) = 861 then value
            else null
        end
    ) as value_4,
    avg(
        case
            when cast(id as int) = 984 then value
            else null
        end
    ) as value_5,
    avg(
        case
            when cast(id as int) = 830 then value
            else null
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
    10000;
