attach 'results/default.db' as base (read_only);

with warm as (
    select
        r.db_version,
        r.suite,
        r.suite_scale_factor as sf,
        s.query_name,
        min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0 as ms
    from run_step s
    join run r on r.id = s.run_id
    where r.db = 'monetdb'
      and r.status = 'completed'
      and s.step_type = 'query'
      and s.status = 'completed'
      and s.iteration_role = 'warm'
    group by all
),
sp3 as (
    select
        r.suite,
        r.suite_scale_factor as sf,
        s.query_name,
        min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0 as ms
    from base.run_step s
    join base.run r on r.id = s.run_id
    where r.db = 'monetdb'
      and r.status = 'completed'
      and s.step_type = 'query'
      and s.status = 'completed'
      and s.iteration_role = 'warm'
    group by all
)
select
    w.db_version,
    w.suite,
    w.sf,
    count(*) as queries,
    round(sum(sp3.ms), 1) as sp3_ms,
    round(sum(w.ms), 1) as new_ms,
    round(sum(sp3.ms) / sum(w.ms), 2) as speedup,
    sum(case when w.ms > sp3.ms * 1.2 then 1 else 0 end) as slower_20pct,
    sum(case when w.ms < sp3.ms / 1.2 then 1 else 0 end) as faster_20pct
from warm w
join sp3 on sp3.suite = w.suite and sp3.sf = w.sf and sp3.query_name = w.query_name
group by all
order by w.suite, w.sf, w.db_version;
