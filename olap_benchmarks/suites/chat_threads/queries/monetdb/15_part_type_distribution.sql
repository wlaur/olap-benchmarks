select part_type, count(*) as parts
from (
    select json.text(json.filter(json.filter(json.filter(m.content, '$.parts'), g.value), '$.type')) as part_type
    from chat_message m
    cross join (select value from generate_series(0, 8)) g
    where g.value < json.length(json.filter(m.content, '$.parts'))
) as exploded
where part_type <> ''
group by part_type
order by parts desc, part_type;
