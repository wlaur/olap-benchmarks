select count(*) as matches, count(distinct thread_id) as threads
from chat_message
where cast(content as text) like '%partition pruning%';
