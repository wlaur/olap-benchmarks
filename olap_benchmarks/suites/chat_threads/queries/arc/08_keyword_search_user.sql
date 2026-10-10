select
    thread_id,
    seq,
    time as created_at
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and json_extract_string(content, '$.parts[0].text') like '%partition pruning%'
order by time desc, thread_id, seq
limit 50;
