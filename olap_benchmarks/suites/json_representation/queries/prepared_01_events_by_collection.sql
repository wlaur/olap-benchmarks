SELECT
  collection AS event,
  count(*) AS count
FROM json_prepared
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
