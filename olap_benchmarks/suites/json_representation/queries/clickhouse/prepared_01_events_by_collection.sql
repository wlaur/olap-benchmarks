SELECT
  collection AS event,
  toInt64(count()) AS count
FROM json_prepared
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
