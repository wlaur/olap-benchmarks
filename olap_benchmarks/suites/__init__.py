from abc import ABC, abstractmethod

from pydantic import BaseModel

from ..dbs import Database
from ..settings import SuiteName


class BenchmarkSuite[DatabaseT: Database](BaseModel, ABC):
    db: DatabaseT
    name: SuiteName

    @abstractmethod
    def populate(self) -> None: ...

    @abstractmethod
    def run(self) -> None: ...
