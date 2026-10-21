WITH extracted AS (
  SELECT json_extract_string(j, ['$.commit.collection', '$.did']) AS fields
  FROM bluesky
  WHERE
    (j ->> '$.kind') = 'commit'
    AND (j ->> '$.commit.operation') = 'create'
)
SELECT
  fields[1] AS event,
  count(*) AS count,
  count(DISTINCT fields[2]) AS users
FROM extracted
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
