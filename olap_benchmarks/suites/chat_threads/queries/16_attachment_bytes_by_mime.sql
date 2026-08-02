-- attachments appear directly on a message and nested inside tool results, so both
-- explicit paths are unioned. Recursive descent is deliberately avoided here.
select
    media_type,
    count(*) as attachments,
    sum(strlen(payload)) as base64_bytes
from (
    select
        unnest(list_concat(
            coalesce(json_extract_string(content, '$.parts[*].source.media_type'), []),
            coalesce(json_extract_string(content, '$.parts[*].content[*].source.media_type'), [])
        )) as media_type,
        unnest(list_concat(
            coalesce(json_extract_string(content, '$.parts[*].source.data'), []),
            coalesce(json_extract_string(content, '$.parts[*].content[*].source.data'), [])
        )) as payload
    from chat_message
) as exploded
group by media_type
order by base64_bytes desc, media_type;
