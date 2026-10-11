import polars as pl

from ...suites.time_series_metrics.config import metric_name


def execute_time_series_metrics_query(source: pl.LazyFrame, query_name: str) -> pl.LazyFrame:
    family, raw_number, _ = query_name.split("_", 2)
    number = int(raw_number)
    is_eav = family.startswith("eav")
    metrics = int(family.removeprefix("eav").removeprefix("wide"))
    if number in (13, 14):
        frame = source
        if number == 14:
            frame = frame.filter(
                (pl.col("time") >= pl.datetime(2024, 12, 29)) & (pl.col("time") < pl.datetime(2024, 12, 30))
            ).with_columns(pl.col("time").dt.truncate("1h").alias("hr"))
        if is_eav:
            groups = ["hr", "metric_name"] if number == 14 else ["metric_name"]
            return (
                frame.group_by(groups)
                .agg(
                    pl.col("value").count().cast(pl.Int64).alias("cnt"),
                    pl.col("value").cast(pl.Float64).sum().alias("total"),
                )
                .sort(groups)
            )
        columns = [metric_name(index) for index in range(1, metrics + 1)]
        aggregates = [pl.col(name).count().cast(pl.Int64).alias(f"cnt_{name}") for name in columns] + [
            pl.col(name).cast(pl.Float64).sum().alias(f"total_{name}") for name in columns
        ]
        return frame.group_by("hr").agg(aggregates).sort("hr") if number == 14 else frame.select(aggregates)
    if number == 12:
        if is_eav:
            return source.select(
                pl.col("value").count().cast(pl.Int64).alias("cnt"),
                pl.col("value").cast(pl.Float64).sum().alias("total"),
            )
        columns = [pl.col(metric_name(index)) for index in range(1, metrics + 1)]
        return source.select(
            pl.sum_horizontal([column.count().cast(pl.Int64) for column in columns]).alias("cnt"),
            pl.sum_horizontal([column.cast(pl.Float64).sum() for column in columns]).alias("total"),
        )
    multi = number in (4, 7, 8)
    if is_eav:
        if multi:
            names = [metric_name(index) for index in range(1, 5)]
            frame = (
                source.filter(pl.col("metric_name").is_in(names))
                .group_by("time")
                .agg(pl.col("value").filter(pl.col("metric_name") == name).first().alias(name) for name in names)
            )
        else:
            frame = source.filter(pl.col("metric_name") == metric_name(1)).select(
                "time", pl.col("value").alias(metric_name(1))
            )
    else:
        frame = source
    value = pl.col(metric_name(1)).cast(pl.Float64)
    match number:
        case 1:
            return frame.select("time", value.alias("value")).sort("time", descending=True).limit(50)
        case 2:
            return frame.filter(
                (pl.col("time") >= pl.datetime(2024, 12, 29)) & (pl.col("time") < pl.datetime(2024, 12, 30))
            ).select(value.mean().alias("value"), value.count().cast(pl.Int64).alias("cnt"))
        case 3 | 4:
            columns = range(1, 5) if multi else range(1, 2)
            return (
                frame.with_columns(pl.col("time").dt.truncate("1h").alias("hr"))
                .group_by("hr")
                .agg(pl.col(metric_name(index)).cast(pl.Float64).mean().alias(f"value_{index}") for index in columns)
                .sort("hr")
            )
        case 5:
            return frame.select(
                value.min().alias("min_val"),
                value.max().alias("max_val"),
                value.mean().alias("avg_val"),
                value.std().alias("stddev_val"),
                value.count().cast(pl.Int64).alias("cnt"),
            )
        case 6:
            return frame.select("time", value.alias("value")).sort("time").limit(1_000)
        case 7:
            return frame.filter(pl.col("time") == pl.datetime(2024, 12, 29, 12, 30)).select(
                "time", *[pl.col(metric_name(index)).cast(pl.Float64).alias(f"value_{index}") for index in range(1, 5)]
            )
        case 8:
            return frame.filter(pl.col(metric_name(2)) > 10).select(
                value.mean().alias("value"), value.count().cast(pl.Int64).alias("cnt")
            )
        case 9:
            return (
                frame.sort("time")
                .select(
                    "time", value.alias("value"), value.rolling_mean(window_size=60, min_samples=1).alias("rolling_avg")
                )
                .limit(1_000)
            )
        case 10:
            return (
                frame.sort("time")
                .select("time", value.alias("value"), (value - value.shift(1)).alias("delta"))
                .limit(1_000)
            )
        case 11:
            return (
                frame.sort("time")
                .select("time", value.alias("value"), value.shift(1).alias("prev_value"))
                .filter(pl.col("value").is_null() & pl.col("prev_value").is_not_null())
            )
        case _:
            raise ValueError(f"Unsupported Time Series Metrics query: {query_name}")
