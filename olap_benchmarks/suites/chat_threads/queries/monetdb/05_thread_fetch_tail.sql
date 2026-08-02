select
    seq,
    json.text(json.filter(content, '$.role')) as role,
    nullif(json.text(json.filter(json.filter(json.filter(content, '$.parts'), 0), '$.text')), '') as first_text
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by seq desc
limit 10;
