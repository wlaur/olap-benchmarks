SELECT
  j ->> '$.commit.collection' AS event,
  count(*) AS count,
  count(DISTINCT (j ->> '$.did')) AS users
FROM json_raw
WHERE
  (j ->> '$.kind') = 'commit'
  AND (j ->> '$.commit.operation') = 'create'
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
