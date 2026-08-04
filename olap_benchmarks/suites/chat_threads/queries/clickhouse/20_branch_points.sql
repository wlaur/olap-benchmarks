select count(*) as branch_points, sum(children) as branched_children
from (
    select parent_id, count(*) as children
    from chat_message
    where parent_id is not null
    group by parent_id
    having count(*) > 1
) as branches;
