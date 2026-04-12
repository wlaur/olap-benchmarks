from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import Protocol, cast

from sqlalchemy import Connection, text

from ..settings import TableName

_LOGGER = logging.getLogger(__name__)


class SupportsCommit(Protocol):
    def commit(self) -> object | None: ...


type QueryRecorder = Callable[[str], AbstractContextManager[None]]


def record_query_execution_context(
    query: str,
    recorder_source: object | None,
) -> AbstractContextManager[None]:
    if recorder_source is None:
        return nullcontext()

    info = cast(Mapping[str, object] | None, getattr(recorder_source, "info", None))
    if not isinstance(info, Mapping):
        return nullcontext()

    recorder_obj = info.get("olap_query_recorder")
    if callable(recorder_obj):
        recorder = cast(QueryRecorder, recorder_obj)
        return recorder(query)

    return nullcontext()


def tracked_commit(connection: SupportsCommit, recorder_source: object | None = None) -> None:
    with record_query_execution_context("COMMIT", recorder_source or connection):
        connection.commit()


def drop_table(table: TableName, connection: Connection, commit: bool = True) -> None:
    connection.execute(text(f'drop table if exists "{table}"'))

    if commit:
        tracked_commit(connection)
