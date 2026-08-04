with exploded as (
    select m.seq as seq, p as part
    from chat_message m, lateral jsonb_array_elements(m.content->'parts') p
    where m.user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e' and m.thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
),
payloads as (
    select seq, octet_length(part->'source'->>'data') as payload_bytes
    from exploded
    where part->'source'->>'data' is not null
    union all
    select e.seq, octet_length(c->'source'->>'data')
    from exploded e, lateral jsonb_array_elements(coalesce(e.part->'content', '[]'::jsonb)) c
    where c->'source'->>'data' is not null
),
totals as (select seq, sum(payload_bytes) as attachment_bytes from payloads group by seq)
select
    m.seq,
    m.content->'parts'->0->>'text' as text_part,
    coalesce(t.attachment_bytes, 0)::bigint as attachment_bytes
from chat_message m
left join totals t on t.seq = m.seq
where m.user_id = 'fd71b9e4-4ea8-5b61-aeb6-cfd65f2da38e' and m.thread_id = '85642b7f-067e-5388-8239-7ae73175d5b9'
order by m.seq;
