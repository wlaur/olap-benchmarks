select tool_name, count(*) as calls
from (
    select arrayJoin(content.parts[].name.:String) as tool_name
    from chat_message
    where content.role = 'assistant'
) as exploded
where isNotNull(tool_name)
group by tool_name
order by calls desc, tool_name;
