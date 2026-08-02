select tool_name, count(*) as calls
from (
    select json.text(json.filter(json.filter(json.filter(m.content, '$.parts'), g.value), '$.name')) as tool_name
    from chat_message m
    cross join (select value from generate_series(0, 8)) g
    where g.value < json.length(json.filter(m.content, '$.parts'))
      and json.text(json.filter(m.content, '$.role')) = 'assistant'
) as exploded
where tool_name <> ''
group by tool_name
order by calls desc, tool_name;
