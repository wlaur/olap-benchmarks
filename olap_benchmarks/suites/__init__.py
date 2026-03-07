from abc import ABC, abstractmethod

from pydantic import BaseModel

from ..dbs import Database
from ..settings import SuiteName


class BenchmarkSuite[DBT: Database](BaseModel, ABC):
    db: DBT
    name: SuiteName

    @abstractmethod
    def populate(self) -> None: ...

    @abstractmethod
    def run(self) -> None: ...
