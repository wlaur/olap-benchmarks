with first_reply as (
    select
        m.thread_id,
        t.created_at as thread_created_at,
        min(m.created_at) as first_assistant_at
    from chat_message m
    join chat_thread t on t.thread_id = m.thread_id
    where json_extract_string(m.content, '$.role') = 'assistant'
      and t.created_at >= timestamp '2025-10-01'
    group by m.thread_id, t.created_at
)
select
    cast(thread_created_at as date) as day,
    count(*) as threads,
    sum(epoch_ms(first_assistant_at) - epoch_ms(thread_created_at)) as total_ms
from first_reply
group by 1
order by 1;
