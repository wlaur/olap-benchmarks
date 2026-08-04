with reply_gaps as (
    select
        created_at,
        content->>'role' as role,
        lag(created_at) over (partition by thread_id order by seq) as previous_at,
        lag(content->>'role') over (partition by thread_id order by seq) as previous_role
    from chat_message
)
select
    count(*) as reply_pairs,
    sum(((extract(epoch from created_at) - extract(epoch from previous_at)) * 1000))::bigint as total_ms,
    max(((extract(epoch from created_at) - extract(epoch from previous_at)) * 1000))::bigint as max_ms
from reply_gaps
where role = 'assistant' and previous_role = 'user';
