select count(*) as messages, count(distinct thread_id) as threads
from chat_message
where content->'parts' @> '[{"type":"visualization"}]'::jsonb;
