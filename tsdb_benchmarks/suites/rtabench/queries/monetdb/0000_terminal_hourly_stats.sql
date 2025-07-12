with hourly_stats as (
select
	date_trunc('hour', event_created) as "hour",
	trim(json.filter(event_payload, '$.terminal'), '"') as terminal,
	count(*) as event_count
from
	order_events
where
	event_created >= '2024-01-01'
	and event_created < '2024-02-01'
	and event_type in ('Created', 'Departed', 'Delivered')
group by
	"hour",
	terminal
)
select
	"hour",
	terminal,
	event_count,
	avg(event_count) over (
    partition by terminal
order by
	"hour"
    rows between 3 preceding and current row
  ) as moving_avg_events
from
	hourly_stats
where
	terminal in ('Berlin', 'Hamburg', 'Munich')
order by
	terminal,
	"hour"
