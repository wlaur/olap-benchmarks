select media_type, count(*) as attachments, sum(length(assumeNotNull(payload))) as base64_bytes
from (
    select
        arrayJoin(arrayZip(
            arrayConcat(content.parts[].source.media_type.:String,
                        arrayFlatten(content.parts[].content[].source.media_type.:String)),
            arrayConcat(content.parts[].source.data.:String,
                        arrayFlatten(content.parts[].content[].source.data.:String))
        )) as pair,
        pair.1 as media_type,
        pair.2 as payload
    from chat_message
) as exploded
where isNotNull(media_type)
group by media_type
order by base64_bytes desc, media_type;
