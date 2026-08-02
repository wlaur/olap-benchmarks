-- "day" is reserved in MonetDB and must be quoted
select
    cast(created_at as date) as "day",
    json.text(json.filter(content, '$.role')) as role,
    count(*) as messages
from chat_message
where created_at >= timestamp '2025-07-01 00:00:00'
group by "day", role
order by "day", role;
