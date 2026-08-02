select thread_id, seq, created_at
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and coalesce(content.parts[].text.:String[1], '') like '%partition pruning%'
order by created_at desc, thread_id, seq
limit 50;
