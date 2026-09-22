attach 'results/default.db' as base (read_only);
with warm as (
  select r.suite, r.suite_scale_factor sf, s.query_name,
         min(datediff('microsecond', s.started_at, s.finished_at))/1000.0 ms
  from run_step s join run r on r.id=s.run_id
  where r.db='monetdb' and r.status='completed' and s.step_type='query'
    and s.status='completed' and s.iteration_role='warm' group by all),
sp3 as (
  select r.suite, r.suite_scale_factor sf, s.query_name,
         min(datediff('microsecond', s.started_at, s.finished_at))/1000.0 ms
  from base.run_step s join base.run r on r.id=s.run_id
  where r.db='monetdb' and r.status='completed' and s.step_type='query'
    and s.status='completed' and s.iteration_role='warm' group by all)
select w.suite, w.query_name, round(sp3.ms,1) as sp3_ms, round(w.ms,1) as new_ms,
       round(w.ms - sp3.ms, 1) as delta_ms, round(sp3.ms/w.ms, 2) as speedup
from warm w join sp3 on sp3.suite=w.suite and sp3.sf=w.sf and sp3.query_name=w.query_name
where abs(w.ms - sp3.ms) > 150
order by (w.ms - sp3.ms) desc;
