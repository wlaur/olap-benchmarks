SELECT
  nullIf(data.commit.collection::String, '') AS event,
  count() AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC;
