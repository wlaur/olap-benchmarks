from abc import ABC, abstractmethod

import polars as pl
from pydantic import BaseModel
from sqlalchemy import Connection

from .settings import DatabaseName, TableName


class Database(BaseModel, ABC):
    name: DatabaseName

    @property
    @abstractmethod
    def start(self) -> str: ...

    @property
    def stop(self) -> str:
        return f"docker stop {self.name}-benchmark"

    @property
    def restart(self) -> str:
        return f"docker restart {self.name}-benchmark"

    _connection: Connection | None = None

    @abstractmethod
    def connect(self, reconnect: bool = False) -> Connection: ...

    @abstractmethod
    def fetch(self, query: str, schema: dict[str, pl.DataType | type[pl.DataType]] | None = None) -> pl.DataFrame: ...

    @abstractmethod
    def insert(self, df: pl.DataFrame, table: TableName) -> None: ...

    @abstractmethod
    def upsert(self, df: pl.DataFrame, table: TableName) -> None: ...
