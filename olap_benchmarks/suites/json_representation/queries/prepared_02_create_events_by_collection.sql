SELECT
  collection AS event,
  count(*) AS count,
  count(DISTINCT (did)) AS users
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
