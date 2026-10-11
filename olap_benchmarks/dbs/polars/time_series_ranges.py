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
        if query.operation == "avg":
            return (
                frame.group_by("metric_name")
                .agg(pl.col("value").cast(pl.Float64).mean().alias("avg_value"))
                .sort("metric_name")
            )
        frame = frame.group_by("time").agg(
            pl.col("value").filter(pl.col("metric_name") == name).cast(pl.Float64).max().alias(name) for name in columns
        )
    elif query.operation == "raw":
        return frame.select("time", *[pl.col(name).cast(pl.Float64) for name in columns]).sort("time")
    elif query.operation == "avg":
        return frame.select(pl.col(name).cast(pl.Float64).mean().alias(f"avg_{name}") for name in columns)

    values = [pl.col(name).cast(pl.Float64) for name in columns]
    balance = (
        values[0] * (values[1] - values[2]) + values[3] * (values[4] - values[5]) - (values[6] * values[7]).abs()
    ) / (1.0 + values[8].abs())
    score = (
        pl.when(values[9].is_null())
        .then(None)
        .when(values[9] > 0.0)
        .then(1.25 * balance)
        .otherwise(0.75 * balance)
        .alias("score")
    )
    return (
        frame.select(pl.col("time").dt.truncate("1h").alias("hr"), score)
        .group_by("hr")
        .agg(
            pl.len().alias("samples"),
            pl.col("score").count().alias("valid_samples"),
            pl.col("score").mean().alias("avg_score"),
            pl.col("score").min().alias("min_score"),
            pl.col("score").max().alias("max_score"),
            (pl.col("score") > 0).fill_null(False).sum().alias("positive_samples"),
        )
        .sort("hr")
    )
