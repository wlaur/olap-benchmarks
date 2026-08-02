select
    case
        when message_count < 5 then '1_under_5'
        when message_count < 15 then '2_5_to_14'
        when message_count < 40 then '3_15_to_39'
        when message_count < 100 then '4_40_to_99'
        else '5_100_plus'
    end as bucket,
    count(*) as threads,
    sum(message_count) as messages
from chat_thread
group by bucket
order by bucket;
