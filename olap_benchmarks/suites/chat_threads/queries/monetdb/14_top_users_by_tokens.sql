with usage_by_user as (
    select
        user_id,
        count(*) as assistant_messages,
        sum(json."integer"(json.filter(content, '$.usage.output_tokens'))) as output_tokens
    from chat_message
    where json.text(json.filter(content, '$.role')) = 'assistant'
    group by user_id
),
threads as (
    select user_id, count(*) as thread_count from chat_thread group by user_id
)
select u.user_id, t.thread_count, u.assistant_messages, u.output_tokens
from usage_by_user u
join threads t on t.user_id = u.user_id
order by u.output_tokens desc, u.user_id
limit 100;
