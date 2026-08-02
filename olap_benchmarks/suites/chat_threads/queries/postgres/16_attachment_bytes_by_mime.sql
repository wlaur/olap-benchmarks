with exploded as (
    select p as part from chat_message m, lateral jsonb_array_elements(m.content->'parts') p
),
sources as (
    select part->'source' as source from exploded where part->'source' is not null
    union all
    select c->'source'
    from exploded e, lateral jsonb_array_elements(coalesce(e.part->'content', '[]'::jsonb)) c
    where c->'source' is not null
)
select
    source->>'media_type' as media_type,
    count(*) as attachments,
    sum(octet_length(source->>'data')) as base64_bytes
from sources
where source->>'media_type' is not null
group by 1
order by base64_bytes desc, media_type;
