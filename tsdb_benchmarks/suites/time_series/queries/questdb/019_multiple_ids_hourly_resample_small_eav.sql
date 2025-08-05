select
    time,
    avg(
        case
            when cast(id as int) = 68 then value
            else null
        end
    ) as value_1,
    avg(
        case
            when cast(id as int) = 10 then cast(value as int)
            else null
        end
    ) as value_2,
    avg(
        case
            when cast(id as int) = 32 then value
            else null
        end
    ) as value_3,
    avg(
        case
            when cast(id as int) = 25 then value
            else null
        end
    ) as value_4,
    avg(
        case
            when cast(id as int) = 53 then value
            else null
        end
    ) as value_5,
    avg(
        case
            when cast(id as int) = 65 then value
            else null
        end
    ) as value_6
from
    (
        select
            date_trunc('hour', time) as time,
            id,
            value
        from
            data_small_eav
        where
            id in (68, 10, 32, 25, 53, 65)
    )
group by
    time
order by
    time
limit
    100;
