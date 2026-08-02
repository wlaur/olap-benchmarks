select
    multiIf(message_count < 5, '1_under_5', message_count < 15, '2_5_to_14',
            message_count < 40, '3_15_to_39', message_count < 100, '4_40_to_99', '5_100_plus') as bucket,
    count(*) as threads,
    sum(message_count) as messages
from chat_thread final
group by bucket
order by bucket;
