select count(*) as matches, count(distinct thread_id) as threads
from chat_message
where toString(content) like '%partition pruning%';
