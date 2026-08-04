select p->>'name' as tool_name, count(*) as calls
from chat_message m, lateral jsonb_array_elements(m.content->'parts') p
where m.content->>'role' = 'assistant' and p->>'name' is not null
group by 1
order by calls desc, tool_name;
