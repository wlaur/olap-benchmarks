select
    content->>'model' as model,
    count(*) as messages,
    sum((content->'usage'->>'input_tokens')::bigint)::bigint as input_tokens,
    sum((content->'usage'->>'output_tokens')::bigint)::bigint as output_tokens
from chat_message
where content->>'role' = 'assistant'
group by 1
order by output_tokens desc, model;
