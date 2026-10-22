WITH extracted AS (
  SELECT json_extract_string(j, ['$.commit.collection', '$.time_us']) AS fields
  FROM bluesky
  WHERE
    (j ->> '$.kind') = 'commit'
    AND (j ->> '$.commit.operation') = 'create'
    AND (j ->> '$.commit.collection') IN (
      'app.bsky.feed.post',
      'app.bsky.feed.repost',
      'app.bsky.feed.like'
    )
)
SELECT
  fields[1] AS event,
  hour(to_timestamp(CAST(fields[2] AS BIGINT) / 1000000) AT TIME ZONE 'UTC') AS hour_of_day,
  count(*) AS count
FROM extracted
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;
