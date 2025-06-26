import polars as pl


def fetch(query: str) -> pl.DataFrame:
    raise NotImplementedError


def fetch_binary(query: str) -> pl.DataFrame:
    raise NotImplementedError
