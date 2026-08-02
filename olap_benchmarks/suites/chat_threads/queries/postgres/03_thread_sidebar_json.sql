select thread_id, settings->>'topic' as topic, settings->>'model_preference' as model_preference, updated_at
from chat_thread
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and (settings->>'archived')::boolean = false
order by updated_at desc, thread_id
limit 50;
