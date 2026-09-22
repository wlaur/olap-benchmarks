attach 'results/default.db' as base (read_only);
attach 'results/master2-pp.db' as pp (read_only);

with warm as (
    select 'sp3' as cfg, r.suite, r.suite_scale_factor as sf, s.query_name,
           min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0 as ms
    from base.run_step s join base.run r on r.id = s.run_id
    where r.db = 'monetdb' and r.status = 'completed' and s.step_type = 'query'
      and s.status = 'completed' and s.result_status = 'ok' and s.iteration_role = 'warm'
    group by all
    union all
    select 'clickhouse', r.suite, r.suite_scale_factor, s.query_name,
           min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0
    from base.run_step s join base.run r on r.id = s.run_id
    where r.db = 'clickhouse' and r.status = 'completed' and s.step_type = 'query'
      and s.status = 'completed' and s.result_status = 'ok' and s.iteration_role = 'warm'
    group by all
    union all
    select 'master_off', r.suite, r.suite_scale_factor, s.query_name,
           min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0
    from run_step s join run r on r.id = s.run_id
    where r.db = 'monetdb' and r.status = 'completed' and s.step_type = 'query'
      and s.status = 'completed' and s.result_status = 'ok' and s.iteration_role = 'warm'
    group by all
    union all
    select 'master_on', r.suite, r.suite_scale_factor, s.query_name,
           min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0
    from pp.run_step s join pp.run r on r.id = s.run_id
    where r.db = 'monetdb' and r.status = 'completed' and s.step_type = 'query'
      and s.status = 'completed' and s.result_status = 'ok' and s.iteration_role = 'warm'
    group by all
),
p as (
    select suite, sf, query_name,
           max(ms) filter (where cfg = 'sp3') as sp3,
           max(ms) filter (where cfg = 'master_off') as moff,
           max(ms) filter (where cfg = 'master_on') as mon,
           max(ms) filter (where cfg = 'clickhouse') as ch
    from warm group by all
)
select suite, query_name,
       round(sp3, 1) as sp3_ms,
       round(moff, 1) as off_ms,
       round(mon, 1) as on_ms,
       round(ch, 1) as ch_ms,
       round(sp3 / moff, 2) as off_x,
       round(sp3 / mon, 2) as on_x
from p
where sp3 is not null and (moff is not null or mon is not null)
  and (abs(coalesce(moff, sp3) - sp3) > 100 or abs(coalesce(mon, sp3) - sp3) > 100)
order by greatest(coalesce(moff, 0), coalesce(mon, 0)) - sp3 desc;
