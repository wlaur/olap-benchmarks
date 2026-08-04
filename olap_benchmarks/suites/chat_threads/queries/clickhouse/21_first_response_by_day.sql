with first_reply as (
    select m.thread_id as thread_id, t.created_at as thread_created_at, min(m.created_at) as first_assistant_at
    from chat_message m
    join chat_thread as t final on t.thread_id = m.thread_id
    where m.content.role = 'assistant' and t.created_at >= toDateTime64('2025-10-01 00:00:00', 3)
    group by m.thread_id, t.created_at
)
select
    toDate(thread_created_at) as day,
    count(*) as threads,
    sum(toUnixTimestamp64Milli(first_assistant_at) - toUnixTimestamp64Milli(thread_created_at)) as total_ms
from first_reply
group by day
order by day;
