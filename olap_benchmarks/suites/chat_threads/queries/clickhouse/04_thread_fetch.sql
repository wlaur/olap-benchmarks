select
    seq,
    content.role as role,
    nullIf(content.model, '') as model,
    length(content.parts[].type.:String) as part_count,
    length(coalesce(content.parts[].text.:String[1], '')) as text_bytes
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e' and thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by seq;
