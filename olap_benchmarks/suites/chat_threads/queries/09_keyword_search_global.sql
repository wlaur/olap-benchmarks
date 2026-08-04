select
    count(*) as matches,
    count(distinct thread_id) as threads
from chat_message
where content like '%partition pruning%';
