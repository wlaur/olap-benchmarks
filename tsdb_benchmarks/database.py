from abc import ABC, abstractmethod

from pydantic import BaseModel
from sqlalchemy import Connection


class Database(BaseModel, ABC):
    name: str

    start_command: str
    stop_command: str

    @abstractmethod
    def connect(self) -> Connection: ...

    class Config:
        extra = "forbid"
