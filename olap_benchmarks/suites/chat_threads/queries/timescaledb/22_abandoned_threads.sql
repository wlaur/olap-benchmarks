with last_message as (
    select distinct on (thread_id) thread_id, content->>'role' as last_role
    from chat_message
    order by thread_id, seq desc
)
select last_role, count(*) as threads
from last_message
group by last_role
order by threads desc, last_role;
