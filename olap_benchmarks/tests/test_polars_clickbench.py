from __future__ import annotations

from datetime import date, datetime

import polars as pl

from olap_benchmarks.dbs.polars.clickbench import execute_clickbench_query
from olap_benchmarks.suites.clickbench.config import CLICKBENCH_QUERY_COUNT


def test_all_polars_clickbench_queries_execute() -> None:
    lf = pl.DataFrame(
        {
            "AdvEngineID": [0, 1],
            "ResolutionWidth": [1280, 1920],
            "UserID": [435090932899640449, 2],
            "SearchPhrase": ["google", ""],
            "EventDate": [date(2013, 7, 14), date(2013, 7, 15)],
            "RegionID": [1, 2],
            "MobilePhoneModel": ["model", ""],
            "MobilePhone": [1, 0],
            "SearchEngineID": [0, 2],
            "EventTime": [datetime(2013, 7, 15, 12, 0), datetime(2013, 7, 15, 12, 1)],
            "URL": ["https://google.example/path", "https://example.test"],
            "Title": ["Google result", "Title"],
            "CounterID": [62, 62],
            "Referer": ["https://www.example.test/path", ""],
            "ClientIP": [100, 101],
            "IsRefresh": [0, 0],
            "WatchID": [1, 2],
            "DontCountHits": [0, 0],
            "IsLink": [1, 0],
            "IsDownload": [0, 0],
            "TraficSourceID": [-1, 6],
            "RefererHash": [3594120000172545465, 0],
            "URLHash": [2868770270353813622, 0],
            "WindowClientWidth": [1280, 1920],
            "WindowClientHeight": [720, 1080],
        }
    ).lazy()

    results = [execute_clickbench_query(lf, f"Q{idx}").collect() for idx in range(CLICKBENCH_QUERY_COUNT)]

    assert len(results) == CLICKBENCH_QUERY_COUNT
