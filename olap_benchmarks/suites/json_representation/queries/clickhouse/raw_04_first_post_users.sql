WITH extracted AS (
  SELECT if(JSONType(j, 'kind') = 'Null', NULL, JSON_VALUE(j, '$.kind')) AS kind, if(JSONType(j, 'commit', 'operation') = 'Null', NULL, JSON_VALUE(j, '$.commit.operation')) AS operation, if(JSONType(j, 'commit', 'collection') = 'Null', NULL, JSON_VALUE(j, '$.commit.collection')) AS collection, if(JSONType(j, 'did') = 'Null', NULL, JSON_VALUE(j, '$.did')) AS did, if(JSONType(j, 'time_us') = 'Null', NULL, JSON_VALUE(j, '$.time_us')) AS time_us_text
  FROM json_raw
)
SELECT
  did AS user_id,
  fromUnixTimestamp64Milli(toInt64(sign(toInt64(min(time_us_text))) * intDiv(abs(toInt128(toInt64(min(time_us_text)))), 1000)), 'UTC') AS first_post_ts
FROM extracted
WHERE
  (kind) = 'commit'
  AND (operation) = 'create'
  AND (collection) = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY toInt64(min(time_us_text)) ASC, user_id ASC NULLS LAST
LIMIT 3;
