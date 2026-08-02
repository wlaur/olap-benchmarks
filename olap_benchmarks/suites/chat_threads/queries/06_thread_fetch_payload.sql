-- renders a whole thread: moves the real text over the wire and forces the base64
-- payloads to be read. Never projects the raw document, because engines disagree on
-- json key order, and never uses recursive descent, because the vega-lite spec also
-- carries a "data" key and $..data would silently over-match it.
select
    seq,
    json_extract_string(content, '$.parts[0].text') as text_part,
    coalesce(list_sum(list_transform(json_extract_string(content, '$.parts[*].source.data'), x -> strlen(x))), 0)
        + coalesce(
            list_sum(list_transform(json_extract_string(content, '$.parts[*].content[*].source.data'), x -> strlen(x))),
            0
        ) as attachment_bytes
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by seq;
