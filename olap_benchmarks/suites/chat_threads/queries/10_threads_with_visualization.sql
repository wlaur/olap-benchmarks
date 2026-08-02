select
    count(*) as messages,
    count(distinct thread_id) as threads
from chat_message
where json_contains(json_extract(content, '$.parts[*].type'), '"visualization"');
