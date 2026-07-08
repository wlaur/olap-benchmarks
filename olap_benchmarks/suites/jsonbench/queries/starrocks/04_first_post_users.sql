SELECT
  get_json_string(data, 'did') AS user_id,
  to_datetime(min(get_json_int(data, 'time_us')), 6) AS first_post_ts
FROM bluesky
WHERE
  get_json_string(data, 'kind') = 'commit'
  AND get_json_string(data, 'commit.operation') = 'create'
  AND get_json_string(data, 'commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC
LIMIT 3;
