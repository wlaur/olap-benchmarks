-- no json array containment in MonetDB, so this is a text match on the flattened document,
-- the same approach rtabench/queries/monetdb/0004 already uses
select count(*) as messages, count(distinct thread_id) as threads
from chat_message
where cast(content as text) like '%"type":"visualization"%';
