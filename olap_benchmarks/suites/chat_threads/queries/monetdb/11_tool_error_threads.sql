select count(*) as failed_results, count(distinct thread_id) as threads
from chat_message
where created_at >= timestamp '2025-12-01 00:00:00'
  and cast(json.filter(json.filter(json.filter(content, '$.parts'), 0), '$.is_error') as text) = 'true';
