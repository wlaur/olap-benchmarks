select
    min(process_364) as min_val,
    max(process_364) as max_val,
    avg(process_364) as avg_val,
    sys.stddev_samp(process_364) as stddev_val,
    count(process_364) as cnt
from
    data_large
