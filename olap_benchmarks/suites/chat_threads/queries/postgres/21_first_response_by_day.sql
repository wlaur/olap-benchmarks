with first_reply as (
    select m.thread_id, t.created_at as thread_created_at, min(m.created_at) as first_assistant_at
    from chat_message m
    join chat_thread t on t.thread_id = m.thread_id
    where m.content->>'role' = 'assistant' and t.created_at >= timestamp '2025-10-01 00:00:00'
    group by m.thread_id, t.created_at
)
select
    thread_created_at::date as day,
    count(*) as threads,
    sum(((extract(epoch from first_assistant_at) - extract(epoch from thread_created_at)) * 1000))::bigint as total_ms
from first_reply
group by 1
order by 1;
