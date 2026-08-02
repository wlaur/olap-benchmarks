-- MonetDB cannot project array elements ($.parts[*].type returns []), so parts are
-- exploded by index against generate_series. Extracting the parts array once in a CTE
-- rather than per (row, index) is worth ~1.9x here.
with docs as (
    select json.filter(content, '$.parts') as parts from chat_message
)
select part_type, count(*) as parts
from (
    select json.text(json.filter(json.filter(d.parts, g.value), '$.type')) as part_type
    from docs d
    cross join (select value from generate_series(0, 8)) g
    where g.value < json.length(d.parts)
) as exploded
where part_type <> ''
group by part_type
order by parts desc, part_type;
