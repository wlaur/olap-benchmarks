-- "ordered" is reserved in MonetDB. Timestamp subtraction yields a sec_interval that cannot
-- be cast to bigint, so the gap is computed from extract(epoch ...), which keeps milliseconds.
with reply_gaps as (
    select
        extract(epoch from created_at) as epoch_seconds,
        json.text(json.filter(content, '$.role')) as role,
        lag(extract(epoch from created_at)) over (partition by thread_id order by seq) as previous_epoch_seconds,
        lag(json.text(json.filter(content, '$.role'))) over (partition by thread_id order by seq) as previous_role
    from chat_message
)
select
    count(*) as reply_pairs,
    cast(sum((epoch_seconds - previous_epoch_seconds) * 1000) as bigint) as total_ms,
    cast(max((epoch_seconds - previous_epoch_seconds) * 1000) as bigint) as max_ms
from reply_gaps
where role = 'assistant' and previous_role = 'user';
