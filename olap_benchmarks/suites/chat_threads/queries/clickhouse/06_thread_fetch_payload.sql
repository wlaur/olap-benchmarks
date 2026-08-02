select
    seq,
    content.parts[].text.:String[1] as text_part,
    arraySum(arrayMap(x -> length(assumeNotNull(x)), arrayFilter(x -> isNotNull(x), arrayConcat(
        content.parts[].source.data.:String,
        arrayFlatten(content.parts[].content[].source.data.:String)
    )))) as attachment_bytes
from chat_message
where user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e' and thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by seq;
