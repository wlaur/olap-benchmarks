select
    time,
    any_value(
        case
            when id = 371 then value
        end
    ) as value_1,
    cast(
        any_value(
            cast(
                case
                    when id = 364 then value
                end as int
            )
        ) as int
    ) as value_2,
    any_value(
        case
            when id = 407 then value
        end
    ) as value_3,
    any_value(
        case
            when id = 861 then value
        end
    ) as value_4,
    any_value(
        case
            when id = 984 then value
        end
    ) as value_5,
    any_value(
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
    10000;
