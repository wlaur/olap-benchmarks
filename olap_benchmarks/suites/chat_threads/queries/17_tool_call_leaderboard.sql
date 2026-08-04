select
    tool_name,
    count(*) as calls
from (
    select unnest(json_extract_string(content, '$.parts[*].name')) as tool_name
    from chat_message
    where json_extract_string(content, '$.role') = 'assistant'
) as exploded
where tool_name is not null
group by tool_name
order by calls desc, tool_name;
