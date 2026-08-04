select thread_id, seq, content.parts[].text.:String[1] as text_part
from chat_message
where created_at >= toDateTime64('2025-12-15 00:00:00', 3)
  and created_at < toDateTime64('2026-01-01 00:00:00', 3)
  and content.role = 'user'
order by thread_id, seq
limit 20000;
