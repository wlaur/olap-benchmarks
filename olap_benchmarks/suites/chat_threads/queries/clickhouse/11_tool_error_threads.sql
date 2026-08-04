select count(*) as failed_results, count(distinct thread_id) as threads
from chat_message
where created_at >= toDateTime64('2025-12-01 00:00:00', 3)
  and content.parts[].is_error.:Bool[1] = true;
