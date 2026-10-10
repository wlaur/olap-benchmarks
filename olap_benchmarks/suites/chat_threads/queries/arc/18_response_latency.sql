with ordered as (
    select
        thread_id,
        seq,
        time,
        json_extract_string(content, '$.role') as role,
        lag(time) over (partition by thread_id order by seq) as previous_at,
        lag(json_extract_string(content, '$.role')) over (partition by thread_id order by seq) as previous_role
    from chat_message
)
select
    count(*) as reply_pairs,
    sum(epoch_ms(time) - epoch_ms(previous_at)) as total_ms,
    max(epoch_ms(time) - epoch_ms(previous_at)) as max_ms
from ordered
where role = 'assistant'
  and previous_role = 'user';
