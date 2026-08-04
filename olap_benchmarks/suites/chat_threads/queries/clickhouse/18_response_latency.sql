with ordered as (
    select
        created_at,
        content.role as role,
        lagInFrame(created_at) over (partition by thread_id order by seq
            rows between unbounded preceding and unbounded following) as previous_at,
        lagInFrame(content.role) over (partition by thread_id order by seq
            rows between unbounded preceding and unbounded following) as previous_role
    from chat_message
)
select
    count(*) as reply_pairs,
    sum(toUnixTimestamp64Milli(created_at) - toUnixTimestamp64Milli(previous_at)) as total_ms,
    max(toUnixTimestamp64Milli(created_at) - toUnixTimestamp64Milli(previous_at)) as max_ms
from ordered
where role = 'assistant' and previous_role = 'user';
