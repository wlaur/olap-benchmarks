with exploded_parts as (
    select json.filter(json.filter(m.content, '$.parts'), g.value) as part
    from chat_message m
    cross join (select value from generate_series(0, 8)) g
    where g.value < json.length(json.filter(m.content, '$.parts'))
),
sources as (
    select
        json.text(json.filter(part, '$.source.media_type')) as media_type,
        json.text(json.filter(part, '$.source.data')) as payload
    from exploded_parts
    union all
    select
        json.text(json.filter(json.filter(json.filter(part, '$.content'), g2.value), '$.source.media_type')),
        json.text(json.filter(json.filter(json.filter(part, '$.content'), g2.value), '$.source.data'))
    from exploded_parts
    cross join (select value from generate_series(0, 4)) g2
    where g2.value < json.length(json.filter(part, '$.content'))
)
select media_type, count(*) as attachments, sum(octet_length(payload)) as base64_bytes
from sources
where media_type <> ''
group by media_type
order by base64_bytes desc, media_type;
