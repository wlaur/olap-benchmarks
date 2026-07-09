SELECT
  json_extract_string(data, '$.commit.collection') AS event,
  count(*) AS count,
  count(DISTINCT json_extract_string(data, '$.did')) AS users
FROM bluesky
WHERE
  json_extract_string(data, '$.kind') = 'commit'
  AND json_extract_string(data, '$.commit.operation') = 'create'
GROUP BY event
ORDER BY count DESC;
