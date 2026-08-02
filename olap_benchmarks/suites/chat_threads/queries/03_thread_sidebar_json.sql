select
    thread_id,
    json_extract_string(settings, '$.topic') as topic,
    json_extract_string(settings, '$.model_preference') as model_preference,
    updated_at
from chat_thread
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and cast(json_extract(settings, '$.archived') as boolean) = false
order by updated_at desc, thread_id
limit 50;
