select thread_id, title, updated_at, message_count
from chat_thread
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
order by updated_at desc, thread_id
limit 50;
