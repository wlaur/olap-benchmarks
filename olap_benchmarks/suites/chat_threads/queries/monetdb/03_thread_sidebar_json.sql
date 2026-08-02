select
    thread_id,
    json.text(json.filter(settings, '$.topic')) as topic,
    json.text(json.filter(settings, '$.model_preference')) as model_preference,
    updated_at
from chat_thread
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and cast(json.filter(settings, '$.archived') as text) = 'false'
order by updated_at desc, thread_id
limit 50;
