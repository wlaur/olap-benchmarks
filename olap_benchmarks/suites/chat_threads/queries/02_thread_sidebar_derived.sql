-- the same sidebar computed from messages instead of the denormalised counters
select
    thread_id,
    max(created_at) as updated_at,
    count(*) as message_count
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
group by thread_id
order by updated_at desc, thread_id
limit 50;
