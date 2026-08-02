with first_reply as (
    select m.thread_id, t.created_at as thread_created_at, min(m.created_at) as first_assistant_at
    from chat_message m
    join chat_thread t on t.thread_id = m.thread_id
    where json.text(json.filter(m.content, '$.role')) = 'assistant'
      and t.created_at >= timestamp '2025-10-01 00:00:00'
    group by m.thread_id, t.created_at
)
select
    cast(thread_created_at as date) as "day",
    count(*) as threads,
    cast(sum((extract(epoch from first_assistant_at) - extract(epoch from thread_created_at)) * 1000) as bigint) as total_ms
from first_reply
group by "day"
order by "day";
