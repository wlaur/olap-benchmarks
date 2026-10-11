SELECT SUM(list_count([*COLUMNS('metric_.*')])) AS cnt, SUM(list_sum(list_transform([*COLUMNS('metric_.*')], x -> CAST(x AS DOUBLE)))) AS total FROM wide1000
