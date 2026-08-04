select
    content.model as model,
    count(*) as messages,
    sum(content.usage.input_tokens) as input_tokens,
    sum(content.usage.output_tokens) as output_tokens
from chat_message
where content.role = 'assistant'
group by model
order by output_tokens desc, model;
