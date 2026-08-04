with last_message as (
    select
        thread_id,
        arg_max(json_extract_string(content, '$.role'), seq) as last_role
    from chat_message
    group by thread_id
)
select
    last_role,
    count(*) as threads
from last_message
group by last_role
order by threads desc, last_role;
