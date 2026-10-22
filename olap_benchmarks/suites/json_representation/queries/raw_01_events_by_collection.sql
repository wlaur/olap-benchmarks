SELECT
  j ->> '$.commit.collection' AS event,
  count(*) AS count
FROM json_raw
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
