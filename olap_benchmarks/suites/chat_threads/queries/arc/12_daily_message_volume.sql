select
    cast(time as date) as day,
    json_extract_string(content, '$.role') as role,
    count(*) as messages
from chat_message
where time >= timestamp '2025-07-01'
group by 1, 2
order by 1, 2;
