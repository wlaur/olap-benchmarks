select p->>'type' as part_type, count(*) as parts
from chat_message m, lateral jsonb_array_elements(m.content->'parts') p
group by 1
order by parts desc, part_type;
