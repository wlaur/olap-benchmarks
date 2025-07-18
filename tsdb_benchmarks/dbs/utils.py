import logging

from sqlalchemy import Connection, text

from ..settings import TableName

_LOGGER = logging.getLogger(__name__)


def drop_table(table: TableName, connection: Connection, commit: bool = True) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))

    if commit:
        connection.commit()
