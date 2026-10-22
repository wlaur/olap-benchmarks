SELECT
  collection AS event,
  toInt64(count()) AS count,
  toInt64(uniqExact(did)) AS users
FROM json_prepared
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
