from abc import ABC, abstractmethod

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection

from .settings import DatabaseName, TableName


class Database(BaseModel, ABC):
    name: DatabaseName

    start: str
    stop: str

    @abstractmethod
    def connect(self) -> Connection: ...

    @abstractmethod
    def fetch(self, query: str) -> pl.DataFrame: ...

    @abstractmethod
    def insert(self, df: pl.DataFrame, table: TableName) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName) -> None: ...
