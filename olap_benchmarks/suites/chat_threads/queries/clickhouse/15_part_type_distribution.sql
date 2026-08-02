select part_type, count(*) as parts
from (select arrayJoin(content.parts[].type.:String) as part_type from chat_message) as exploded
group by part_type
order by parts desc, part_type;
