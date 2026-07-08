SELECT
  get_json_string(data, 'did') AS user_id,
  date_diff(
    'millisecond',
    to_datetime(min(get_json_int(data, 'time_us')), 6),
    to_datetime(max(get_json_int(data, 'time_us')), 6)
  ) AS activity_span
FROM bluesky
WHERE
  get_json_string(data, 'kind') = 'commit'
  AND get_json_string(data, 'commit.operation') = 'create'
  AND get_json_string(data, 'commit.collection') = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC
LIMIT 3;
