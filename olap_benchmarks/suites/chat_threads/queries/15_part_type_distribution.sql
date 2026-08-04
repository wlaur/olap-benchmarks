select
    part_type,
    count(*) as parts
from (
    select unnest(json_extract_string(content, '$.parts[*].type')) as part_type
    from chat_message
) as exploded
group by part_type
order by parts desc, part_type;
