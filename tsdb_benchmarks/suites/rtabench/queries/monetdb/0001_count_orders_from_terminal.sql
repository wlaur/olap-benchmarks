select
	date_trunc('day', event_created) as "day",
	count(*) as count
from
	order_events
where
	event_created >= '2024-04-01'
	and event_created < '2024-05-01'
	and event_type = 'Departed'
	and trim(json.filter(event_payload,
	'$.terminal'), '"') = 'Berlin'
group by
	"day"
order by
	count desc,
	"day";
