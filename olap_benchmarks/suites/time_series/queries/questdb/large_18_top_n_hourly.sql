-- QuestDB has no NULLS LAST and sorts NULL first under DESC, so an all-NULL hour is ranked after
-- every other hour through an explicit sort key.
select
    hr,
    avg_value
from
    (
        select
            date_trunc('hour', time) as hr,
            avg(process_364) as avg_value,
            case when avg(process_364) is null then 1 else 0 end as null_last
        from
            data_large
        group by
            date_trunc('hour', time)
    )
order by
    null_last,
    avg_value desc
limit
    10
