SELECT
  data.commit.collection AS event,
  count() AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC;
