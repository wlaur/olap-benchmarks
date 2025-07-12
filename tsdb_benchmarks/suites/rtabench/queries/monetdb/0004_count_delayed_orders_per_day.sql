with cte as (
select
	date_trunc('day', event_created) as "day",
	cast(json.filter(event_payload, '$.status') as text) as status
from
	order_events
where
	event_created >= '2024-05-01'
	and event_created < '2024-06-01'
)
select
	"day",
	count(*) as count
from
	cte
where
  -- MonetDB does not have JSON list contains or similar
	status like '%"Delayed"%'
	and status like '%"Priority"%'
group by
	"day"
order by
	count desc,
	"day"
limit 20;
