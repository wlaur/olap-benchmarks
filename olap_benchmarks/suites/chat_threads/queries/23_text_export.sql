select
    thread_id,
    seq,
    json_extract_string(content, '$.parts[0].text') as text_part
from chat_message
where created_at >= timestamp '2025-12-15'
  and created_at < timestamp '2026-01-01'
  and json_extract_string(content, '$.role') = 'user'
order by thread_id, seq
limit 20000;
