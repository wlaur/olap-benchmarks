import logging
from time import perf_counter, sleep

from sqlalchemy import Connection, create_engine, text

from ..settings import TableName

_LOGGER = logging.getLogger(__name__)


def drop_table(table: TableName, connection: Connection, commit: bool = True) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))

    if commit:
        connection.commit()


def wait_for_sqlalchemy_connection(
    connection_string: str, timeout_seconds: float = 30.0, interval_seconds: float = 0.5
) -> None:
    _LOGGER.info("Waiting for database to accept SQL connections...")

    deadline = perf_counter() + timeout_seconds

    while perf_counter() < deadline:
        try:
            engine = create_engine(connection_string)
            with engine.connect() as connection:
                connection.execute(text("select 1"))
            _LOGGER.info("Database is ready to accept connections")
            return
        except Exception as e:
            _LOGGER.debug(f"Database not ready yet: {e}")
            sleep(interval_seconds)

    raise TimeoutError("Timed out waiting for database to become ready")
