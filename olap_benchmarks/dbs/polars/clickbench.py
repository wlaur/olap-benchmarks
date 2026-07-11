from __future__ import annotations

from datetime import date

import polars as pl


def _count(name: str = "count") -> pl.Expr:
    return pl.len().cast(pl.Int64).alias(name)


def _july_filter() -> pl.Expr:
    return (
        (pl.col("CounterID") == 62)
        & pl.col("EventDate").is_between(date(2013, 7, 1), date(2013, 7, 31))
        & (pl.col("IsRefresh") == 0)
    )


def execute_clickbench_query(lf: pl.LazyFrame, query_name: str) -> pl.LazyFrame:
    match query_name:
        case "Q0":
            return lf.select(_count())
        case "Q1":
            return lf.filter(pl.col("AdvEngineID") != 0).select(_count())
        case "Q2":
            return lf.select(pl.col("AdvEngineID").sum(), _count(), pl.col("ResolutionWidth").mean())
        case "Q3":
            return lf.select(pl.col("UserID").mean())
        case "Q4":
            return lf.select(pl.col("UserID").n_unique().cast(pl.Int64))
        case "Q5":
            return lf.select(pl.col("SearchPhrase").n_unique().cast(pl.Int64))
        case "Q6":
            return lf.select(
                pl.col("EventDate").min().alias("min_event_date"),
                pl.col("EventDate").max().alias("max_event_date"),
            )
        case "Q7":
            return (
                lf.filter(pl.col("AdvEngineID") != 0)
                .group_by("AdvEngineID")
                .agg(_count())
                .sort("count", descending=True)
            )
        case "Q8":
            return (
                lf.group_by("RegionID")
                .agg(pl.col("UserID").n_unique().cast(pl.Int64).alias("u"))
                .sort("u", descending=True)
                .limit(10)
            )
        case "Q9":
            return (
                lf.group_by("RegionID")
                .agg(
                    pl.col("AdvEngineID").sum(),
                    _count("c"),
                    pl.col("ResolutionWidth").mean(),
                    pl.col("UserID").n_unique().cast(pl.Int64),
                )
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q10":
            return (
                lf.filter(pl.col("MobilePhoneModel") != "")
                .group_by("MobilePhoneModel")
                .agg(pl.col("UserID").n_unique().cast(pl.Int64).alias("u"))
                .sort("u", descending=True)
                .limit(10)
            )
        case "Q11":
            return (
                lf.filter(pl.col("MobilePhoneModel") != "")
                .group_by("MobilePhone", "MobilePhoneModel")
                .agg(pl.col("UserID").n_unique().cast(pl.Int64).alias("u"))
                .sort("u", descending=True)
                .limit(10)
            )
        case "Q12":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .group_by("SearchPhrase")
                .agg(_count("c"))
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q13":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .group_by("SearchPhrase")
                .agg(pl.col("UserID").n_unique().cast(pl.Int64).alias("u"))
                .sort("u", descending=True)
                .limit(10)
            )
        case "Q14":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .group_by("SearchEngineID", "SearchPhrase")
                .agg(_count("c"))
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q15":
            return lf.group_by("UserID").agg(_count()).sort("count", descending=True).limit(10)
        case "Q16":
            return lf.group_by("UserID", "SearchPhrase").agg(_count()).sort("count", descending=True).limit(10)
        case "Q17":
            return lf.group_by("UserID", "SearchPhrase", maintain_order=True).agg(_count()).limit(10)
        case "Q18":
            return (
                lf.with_columns(pl.col("EventTime").dt.minute().cast(pl.Int64).alias("m"))
                .group_by("UserID", "m", "SearchPhrase")
                .agg(_count())
                .sort("count", descending=True)
                .limit(10)
            )
        case "Q19":
            return lf.filter(pl.col("UserID") == 435090932899640449).select("UserID")
        case "Q20":
            return lf.filter(pl.col("URL").str.contains("google", literal=True)).select(_count())
        case "Q21":
            return (
                lf.filter(pl.col("URL").str.contains("google", literal=True) & (pl.col("SearchPhrase") != ""))
                .group_by("SearchPhrase")
                .agg(pl.col("URL").min(), _count("c"))
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q22":
            return (
                lf.filter(
                    pl.col("Title").str.contains("Google", literal=True)
                    & ~pl.col("URL").str.contains(".google.", literal=True)
                    & (pl.col("SearchPhrase") != "")
                )
                .group_by("SearchPhrase")
                .agg(
                    pl.col("URL").min(),
                    pl.col("Title").min(),
                    _count("c"),
                    pl.col("UserID").n_unique().cast(pl.Int64),
                )
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q23":
            return lf.filter(pl.col("URL").str.contains("google", literal=True)).sort("EventTime").limit(10)
        case "Q24":
            return lf.filter(pl.col("SearchPhrase") != "").sort("EventTime").select("SearchPhrase").limit(10)
        case "Q25":
            return lf.filter(pl.col("SearchPhrase") != "").sort("SearchPhrase").select("SearchPhrase").limit(10)
        case "Q26":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .sort("EventTime", "SearchPhrase")
                .select("SearchPhrase")
                .limit(10)
            )
        case "Q27":
            return (
                lf.filter(pl.col("URL") != "")
                .group_by("CounterID")
                .agg(pl.col("URL").str.len_chars().mean().alias("l"), _count("c"))
                .filter(pl.col("c") > 100000)
                .sort("l", descending=True)
                .limit(25)
            )
        case "Q28":
            return (
                lf.filter(pl.col("Referer") != "")
                .with_columns(pl.col("Referer").str.replace(r"^https?://(?:www\.)?([^/]+)/.*$", "$1").alias("k"))
                .group_by("k")
                .agg(
                    pl.col("Referer").str.len_chars().mean().alias("l"),
                    _count("c"),
                    pl.col("Referer").min(),
                )
                .filter(pl.col("c") > 100000)
                .sort("l", descending=True)
                .limit(25)
            )
        case "Q29":
            return lf.select((pl.col("ResolutionWidth") + offset).sum().alias(f"sum_{offset}") for offset in range(90))
        case "Q30":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .group_by("SearchEngineID", "ClientIP")
                .agg(_count("c"), pl.col("IsRefresh").sum(), pl.col("ResolutionWidth").mean())
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q31":
            return (
                lf.filter(pl.col("SearchPhrase") != "")
                .group_by("WatchID", "ClientIP")
                .agg(_count("c"), pl.col("IsRefresh").sum(), pl.col("ResolutionWidth").mean())
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q32":
            return (
                lf.group_by("WatchID", "ClientIP")
                .agg(_count("c"), pl.col("IsRefresh").sum(), pl.col("ResolutionWidth").mean())
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q33":
            return lf.group_by("URL").agg(_count("c")).sort("c", descending=True).limit(10)
        case "Q34":
            return (
                lf.group_by("URL")
                .agg(_count("c"))
                .select(pl.lit(1).cast(pl.Int64), "URL", "c")
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q35":
            return (
                lf.group_by("ClientIP")
                .agg(_count("c"))
                .select(
                    "ClientIP",
                    (pl.col("ClientIP") - 1).alias("ClientIP_1"),
                    (pl.col("ClientIP") - 2).alias("ClientIP_2"),
                    (pl.col("ClientIP") - 3).alias("ClientIP_3"),
                    "c",
                )
                .sort("c", descending=True)
                .limit(10)
            )
        case "Q36":
            return (
                lf.filter(_july_filter() & (pl.col("DontCountHits") == 0) & (pl.col("URL") != ""))
                .group_by("URL")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .limit(10)
            )
        case "Q37":
            return (
                lf.filter(_july_filter() & (pl.col("DontCountHits") == 0) & (pl.col("Title") != ""))
                .group_by("Title")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .limit(10)
            )
        case "Q38":
            return (
                lf.filter(_july_filter() & (pl.col("IsLink") != 0) & (pl.col("IsDownload") == 0))
                .group_by("URL")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .slice(1000, 10)
            )
        case "Q39":
            return (
                lf.filter(_july_filter())
                .with_columns(
                    pl.when((pl.col("SearchEngineID") == 0) & (pl.col("AdvEngineID") == 0))
                    .then(pl.col("Referer"))
                    .otherwise(pl.lit(""))
                    .alias("Src"),
                    pl.col("URL").alias("Dst"),
                )
                .group_by("TraficSourceID", "SearchEngineID", "AdvEngineID", "Src", "Dst")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .slice(1000, 10)
            )
        case "Q40":
            return (
                lf.filter(
                    _july_filter()
                    & pl.col("TraficSourceID").is_in([-1, 6])
                    & (pl.col("RefererHash") == 3594120000172545465)
                )
                .group_by("URLHash", "EventDate")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .slice(100, 10)
            )
        case "Q41":
            return (
                lf.filter(_july_filter() & (pl.col("DontCountHits") == 0) & (pl.col("URLHash") == 2868770270353813622))
                .group_by("WindowClientWidth", "WindowClientHeight")
                .agg(_count("PageViews"))
                .sort("PageViews", descending=True)
                .slice(10000, 10)
            )
        case "Q42":
            return (
                lf.filter(
                    (pl.col("CounterID") == 62)
                    & pl.col("EventDate").is_between(date(2013, 7, 14), date(2013, 7, 15))
                    & (pl.col("IsRefresh") == 0)
                    & (pl.col("DontCountHits") == 0)
                )
                .with_columns(pl.col("EventTime").dt.truncate("1m").alias("M"))
                .group_by("M")
                .agg(_count("PageViews"))
                .sort("M")
                .slice(1000, 10)
            )
        case _:
            raise ValueError(f"Unsupported Polars ClickBench query: {query_name}")
