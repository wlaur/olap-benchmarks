-- alternative with self join (slightly or much slower for all dbs)
-- performance depends on how many rows the condition filters out
-- select
--     avg(e1.value) as value
-- from
--     data_large_eav e1
--     join data_large_eav e2 on e1.time = e2.time
-- where
--     e1.id = 371
--     and e2.id = 830
--     and abs(e2.value) < 10;
select
    avg(value) as value
from
    data_large_eav
where
    id = 371
    and time in (
        select
            time
        from
            data_large_eav
        where
            id = 830
            and abs(value) < 10
    )
