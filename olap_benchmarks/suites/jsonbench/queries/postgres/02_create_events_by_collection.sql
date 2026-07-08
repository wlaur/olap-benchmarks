SELECT
  data -> 'commit' ->> 'collection' AS event,
  count(*) AS count,
  count(DISTINCT data ->> 'did') AS users
FROM bluesky
WHERE
  data ->> 'kind' = 'commit'
  AND data -> 'commit' ->> 'operation' = 'create'
GROUP BY event
ORDER BY count DESC;
