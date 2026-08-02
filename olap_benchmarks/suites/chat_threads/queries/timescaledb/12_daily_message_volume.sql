select created_at::date as day, content->>'role' as role, count(*) as messages
from chat_message
where created_at >= timestamp '2025-07-01 00:00:00'
group by 1, 2
order by 1, 2;
