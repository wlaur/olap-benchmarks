select
    json_extract_string(content, '$.model') as model,
    count(*) as messages,
    sum(cast(json_extract(content, '$.usage.input_tokens') as bigint)) as input_tokens,
    sum(cast(json_extract(content, '$.usage.output_tokens') as bigint)) as output_tokens
from chat_message
where json_extract_string(content, '$.role') = 'assistant'
group by 1
order by output_tokens desc, model;
