with usage as (
    select
        user_id,
        count(*) as assistant_messages,
        sum(cast(json_extract(content, '$.usage.output_tokens') as bigint)) as output_tokens
    from chat_message
    where json_extract_string(content, '$.role') = 'assistant'
    group by user_id
),
threads as (
    select user_id, count(*) as thread_count
    from chat_thread
    group by user_id
)
select
    usage.user_id,
    threads.thread_count,
    usage.assistant_messages,
    usage.output_tokens
from usage
join threads on threads.user_id = usage.user_id
order by usage.output_tokens desc, usage.user_id
limit 100;
