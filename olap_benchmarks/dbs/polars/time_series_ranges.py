import polars as pl

from ...suites.time_series_ranges.config import END, RANGE_QUERIES


def execute_time_series_ranges_query(source: pl.LazyFrame, query_name: str) -> pl.LazyFrame:
    family, number, _ = query_name.split("_", 2)
    query = RANGE_QUERIES[int(number) - 1]
    is_eav = family.startswith("eav")
    metrics = int(family.removeprefix("eav").removeprefix("wide"))
    columns = query.columns(metrics)
    frame = source.filter((pl.col("time") >= query.start) & (pl.col("time") < END))
    if is_eav:
        frame = frame.filter(pl.col("metric_name").is_in(columns))
        if query.operation == "raw":
            return frame.select("time", "metric_name", pl.col("value").cast(pl.Float64)).sort("time", "metric_name")
        return (
            frame.group_by("metric_name")
            .agg(pl.col("value").cast(pl.Float64).mean().alias("avg_value"))
            .sort("metric_name")
        )
    if query.operation == "raw":
        return frame.select("time", *[pl.col(name).cast(pl.Float64) for name in columns]).sort("time")
    return frame.select(pl.col(name).cast(pl.Float64).mean().alias(f"avg_{name}") for name in columns)
