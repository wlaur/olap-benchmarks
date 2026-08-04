select
    json.text(json.filter(content, '$.model')) as model,
    count(*) as messages,
    sum(json."integer"(json.filter(content, '$.usage.input_tokens'))) as input_tokens,
    sum(json."integer"(json.filter(content, '$.usage.output_tokens'))) as output_tokens
from chat_message
where json.text(json.filter(content, '$.role')) = 'assistant'
group by model
order by output_tokens desc, model;
