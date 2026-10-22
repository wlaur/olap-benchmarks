WITH extracted AS (
  SELECT if(JSONType(j, 'kind') = 'Null', NULL, JSON_VALUE(j, '$.kind')) AS kind, if(JSONType(j, 'commit', 'operation') = 'Null', NULL, JSON_VALUE(j, '$.commit.operation')) AS operation, if(JSONType(j, 'commit', 'collection') = 'Null', NULL, JSON_VALUE(j, '$.commit.collection')) AS collection, if(JSONType(j, 'did') = 'Null', NULL, JSON_VALUE(j, '$.did')) AS did, if(JSONType(j, 'time_us') = 'Null', NULL, JSON_VALUE(j, '$.time_us')) AS time_us_text
  FROM json_raw
)
SELECT
  collection AS event,
  toInt64(count()) AS count,
  toInt64(uniqExact(did)) AS users
FROM extracted
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
GROUP BY event
ORDER BY count DESC, event ASC NULLS LAST;
