select
    seq,
    json_extract_string(content, '$.role') as role,
    json_extract_string(content, '$.model') as model,
    json_array_length(json_extract(content, '$.parts')) as part_count,
    coalesce(strlen(json_extract_string(content, '$.parts[0].text')), 0) as text_bytes
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by seq;
