SELECT
  get_json_string(data, 'commit.collection') AS event,
  count() AS count,
  count(DISTINCT get_json_string(data, 'did')) AS users
FROM bluesky
WHERE
  get_json_string(data, 'kind') = 'commit'
  AND get_json_string(data, 'commit.operation') = 'create'
GROUP BY event
ORDER BY count DESC;
