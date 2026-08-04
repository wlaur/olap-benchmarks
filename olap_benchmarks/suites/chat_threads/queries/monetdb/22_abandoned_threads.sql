-- no arg_max in MonetDB; the last message per thread is picked with a window function
with ranked as (
    select
        thread_id,
        json.text(json.filter(content, '$.role')) as role,
        row_number() over (partition by thread_id order by seq desc) as rn
    from chat_message
)
select role as last_role, count(*) as threads
from ranked
where rn = 1
group by role
order by threads desc, last_role;
