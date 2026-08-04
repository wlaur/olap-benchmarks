select thread_id, seq, content->'parts'->0->>'text' as text_part
from chat_message
where created_at >= timestamp '2025-12-15 00:00:00'
  and created_at < timestamp '2026-01-01 00:00:00'
  and content->>'role' = 'user'
order by thread_id, seq
limit 20000;
