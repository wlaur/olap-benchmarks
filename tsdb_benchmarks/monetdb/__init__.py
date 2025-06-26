from sqlalchemy import Connection, create_engine

from ..database import Database
from ..settings import SETTINGS


class MonetDB(Database):
    name = "monetdb"
    start_command = (
        f"docker run --name monetdb-benchmark --rm -d -p 50000:50000 "
        f"-v {SETTINGS.database_directory.as_posix()}/monetdb:/var/monetdb5/dbfarm monetdb/monetdb:Mar2025"
    )
    stop_command = "docker kill monetdb-benchmark"

    def connect(self) -> Connection:
        engine = create_engine("monetdb://monetdb:monetdb@localhost:50000/benchmark")
        return engine.connect()
