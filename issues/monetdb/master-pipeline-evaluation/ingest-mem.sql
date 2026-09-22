attach 'results/default.db' as base (read_only);
attach 'results/{{PP}}.db' as pp (read_only);

with steps as (
    select 'sp3' as cfg, r.suite, r.suite_scale_factor as sf, s.step_type, s.step_name, s.table_name,
           datediff('microsecond', s.started_at, s.finished_at) / 1e6 as sec
    from base.run_step s join base.run r on r.id = s.run_id
    where r.db = 'monetdb' and r.operation = 'populate' and s.status = 'completed'
    union all
    select 'master_off', r.suite, r.suite_scale_factor, s.step_type, s.step_name, s.table_name,
           datediff('microsecond', s.started_at, s.finished_at) / 1e6
    from run_step s join run r on r.id = s.run_id
    where r.db = 'monetdb' and r.operation = 'populate' and s.status = 'completed'
    union all
    select 'clickhouse', r.suite, r.suite_scale_factor, s.step_type, s.step_name, s.table_name,
           datediff('microsecond', s.started_at, s.finished_at) / 1e6
    from base.run_step s join base.run r on r.id = s.run_id
    where r.db = 'clickhouse' and r.operation = 'populate' and s.status = 'completed'
),
ing as (
    select cfg, suite, sf, table_name, sum(sec) as sec
    from steps
    where step_type = 'phase' and step_name = 'insert' and table_name is not null
    group by all
)
select suite, sf, table_name,
       round(max(sec) filter (where cfg = 'sp3'), 1) as sp3_s,
       round(max(sec) filter (where cfg = 'master_off'), 1) as master_s,
       round(max(sec) filter (where cfg = 'clickhouse'), 1) as ch_s,
       round(max(sec) filter (where cfg = 'sp3') / max(sec) filter (where cfg = 'master_off'), 2) as master_x
from ing
group by all
having max(sec) filter (where cfg = 'sp3') is not null
   and max(sec) filter (where cfg = 'master_off') is not null
order by max(sec) filter (where cfg = 'sp3') desc;
