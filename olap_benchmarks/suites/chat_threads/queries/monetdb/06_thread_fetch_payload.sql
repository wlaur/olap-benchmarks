-- MonetDB has no array-element projection ($.parts[*].x returns []), so parts are exploded
-- by index. Recursive $..data is deliberately avoided: the vega-lite spec also carries a
-- "data" key and would be counted as attachment bytes.
with exploded as (
    select m.seq as seq, json.filter(json.filter(m.content, '$.parts'), g.value) as part
    from chat_message m
    cross join (select value from generate_series(0, 8)) g
    where m.user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
      and m.thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
      and g.value < json.length(json.filter(m.content, '$.parts'))
),
payloads as (
    select seq, octet_length(json.text(json.filter(part, '$.source.data'))) as payload_bytes
    from exploded
    union all
    select
        e.seq,
        octet_length(json.text(json.filter(json.filter(json.filter(e.part, '$.content'), g2.value), '$.source.data')))
    from exploded e
    cross join (select value from generate_series(0, 4)) g2
    where g2.value < json.length(json.filter(e.part, '$.content'))
),
totals as (
    select seq, sum(payload_bytes) as attachment_bytes from payloads group by seq
)
select
    m.seq,
    nullif(json.text(json.filter(json.filter(json.filter(m.content, '$.parts'), 0), '$.text')), '') as text_part,
    coalesce(t.attachment_bytes, 0) as attachment_bytes
from chat_message m
left join totals t on t.seq = m.seq
where m.user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e'
  and m.thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by m.seq;
