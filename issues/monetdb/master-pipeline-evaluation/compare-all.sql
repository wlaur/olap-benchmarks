attach 'results/default.db' as base (read_only);
attach 'results/{{PP}}.db' as pp (read_only);

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
    select 'duckdb', r.suite, r.suite_scale_factor, s.query_name,
           min(datediff('microsecond', s.started_at, s.finished_at)) / 1000.0
    from base.run_step s join base.run r on r.id = s.run_id
    where r.db = 'duckdb' and r.status = 'completed' and s.step_type = 'query'
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
common as (
    select suite, sf, query_name
    from warm
    group by all
    having count(distinct cfg) = 5
),
w as (select warm.* from warm join common using (suite, sf, query_name))
select
    suite,
    sf,
    count(*) filter (where cfg = 'sp3') as queries,
    round(sum(ms) filter (where cfg = 'sp3') / 1000.0, 2) as sp3_s,
    round(sum(ms) filter (where cfg = 'master_off') / 1000.0, 2) as off_s,
    round(sum(ms) filter (where cfg = 'master_on') / 1000.0, 2) as on_s,
    round(sum(ms) filter (where cfg = 'clickhouse') / 1000.0, 2) as ch_s,
    round(sum(ms) filter (where cfg = 'duckdb') / 1000.0, 2) as duck_s,
    round(sum(ms) filter (where cfg = 'sp3') / sum(ms) filter (where cfg = 'master_off'), 2) as off_vs_sp3,
    round(sum(ms) filter (where cfg = 'sp3') / sum(ms) filter (where cfg = 'master_on'), 2) as on_vs_sp3,
    round(sum(ms) filter (where cfg = 'clickhouse')
          / least(sum(ms) filter (where cfg = 'master_off'), sum(ms) filter (where cfg = 'master_on')), 2)
        as best_vs_ch,
    round(sum(ms) filter (where cfg = 'duckdb')
          / least(sum(ms) filter (where cfg = 'master_off'), sum(ms) filter (where cfg = 'master_on')), 2)
        as best_vs_duck
from w
group by all
order by suite, sf;
