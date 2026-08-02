select toDate(created_at) as day, content.role as role, count(*) as messages
from chat_message
where created_at >= toDateTime64('2025-07-01 00:00:00', 3)
group by day, role
order by day, role;
