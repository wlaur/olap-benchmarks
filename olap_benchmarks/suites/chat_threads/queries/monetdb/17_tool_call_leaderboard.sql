-- role and parts are extracted once per row rather than once per (row, index)
with docs as (
    select
        json.filter(content, '$.parts') as parts,
        json.text(json.filter(content, '$.role')) as role
    from chat_message
)
select tool_name, count(*) as calls
from (
    select json.text(json.filter(json.filter(d.parts, g.value), '$.name')) as tool_name
    from docs d
    cross join (select value from generate_series(0, 8)) g
    where d.role = 'assistant'
      and g.value < json.length(d.parts)
) as exploded
where tool_name <> ''
group by tool_name
order by calls desc, tool_name;
