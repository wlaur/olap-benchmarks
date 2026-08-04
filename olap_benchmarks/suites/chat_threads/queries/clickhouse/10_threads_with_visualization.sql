select count(*) as messages, count(distinct thread_id) as threads
from chat_message
where has(content.parts[].type.:String, 'visualization');
