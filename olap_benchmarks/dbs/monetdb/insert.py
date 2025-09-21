import logging
import shutil
import uuid
from pathlib import Path
from textwrap import dedent
from time import perf_counter

import polars as pl
from sqlalchemy import Connection, text

from ...settings import TableName
from .binary import write_binary_column_data
from .settings import SETTINGS as MONETDB_SETTINGS
from .utils import (
    MONETDB_TEMPORARY_DIRECTORY,
    create_table,
    ensure_downloader_uploader,
    get_pymonetdb_connection,
    get_table,
)

_LOGGER = logging.getLogger(__name__)


def insert(
    df: pl.DataFrame,
    table: TableName,
    connection: Connection,
    primary_key: str | list[str] | None = None,
    not_null: str | list[str] | None = None,
    create: bool = True,
    commit: bool = True,
) -> None:
    t0 = perf_counter()

    if create:
        # NOTE: when inserting into an existing table, the column order and types must match exactly
        # NOTE: using (id, time) primary key or not null for large EAV tables makes insertion orders of magnitude slower
        # using primary key also increases disk usage by 30%, not null does not increase disk usage
        # query performance is the same even if no primary key or not null constraints are used
        create_table(table, df.schema, connection, primary_key, not_null)
        _LOGGER.info(f"Created table '{table}' with {len(df.columns):_} columns")

    con = get_pymonetdb_connection(connection)
    ensure_downloader_uploader(con)

    temp_dir = MONETDB_TEMPORARY_DIRECTORY / "data" / str(uuid.uuid4())[:4]
    temp_dir.mkdir()

    subdir = temp_dir.relative_to(MONETDB_TEMPORARY_DIRECTORY).as_posix()
    column_files: list[Path] = []
    path_prefix = "" if MONETDB_SETTINGS.client_file_transfer else "/"

    try:
        for idx, col in enumerate(df.columns):
            path = temp_dir / f"{idx}.bin"
            write_binary_column_data(df[col], path)
            column_files.append(path)

        files_clause = ", ".join(f"'{path_prefix}{subdir}/{path.name}'" for path in column_files)
        con.execute(
            f"copy little endian binary into {table} from {files_clause} "
            f"on {'client' if MONETDB_SETTINGS.client_file_transfer else 'server'}"
        )

        if commit:
            con.commit()

    except Exception as e:
        col_indexes = dict(enumerate(df.columns))
        raise ValueError(f"Could not insert binary data for '{table}', columns:\n{col_indexes}\n") from e
    finally:
        shutil.rmtree(temp_dir)

    _LOGGER.info(
        f"Inserted dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) "
        f"into table {table} in {perf_counter() - t0:_.2f} seconds"
    )


def upsert(df: pl.DataFrame, table: TableName, connection: Connection, primary_key: str | list[str]) -> None:
    t0 = perf_counter()
    dest = get_table(table, df.schema)

    temp_table_name = f"_temporary_{str(uuid.uuid4())[:4]}"
    source = create_table(temp_table_name, df.schema, connection, primary_key=primary_key, temporary=True)

    insert(df, source.name, connection, create=False, commit=False)

    primary_keys = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    shared_cols = sorted({c.name for c in dest.columns} & {c.name for c in dest.columns})

    if not shared_cols:
        raise ValueError("No overlapping columns to upsert")

    update_cols = [col for col in shared_cols if col not in primary_keys]

    if not update_cols:
        raise ValueError("No non-PK columns to upsert")

    on_clause = " and ".join(f'dest."{pk}" = source."{pk}"' for pk in primary_keys)
    update_assignments = ", ".join(f'"{col}" = source."{col}"' for col in update_cols)

    insert_cols = ", ".join(f'"{col}"' for col in shared_cols)
    insert_values = ", ".join(f'source."{col}"' for col in shared_cols)

    merge_statement = dedent(f"""
        merge into "{dest.name}" as dest
        using "{source.name}" as source
            on {on_clause}
        when matched then
            update set {update_assignments}
        when not matched then
            insert ({insert_cols}) values ({insert_values})
    """)

    connection.execute(text(merge_statement))
    connection.commit()

    _LOGGER.info(
        f"Upserted dataset with shape ({df.shape[0]:_}, {df.shape[1]:_}) "
        f"into table {table} in {perf_counter() - t0:_.2f} seconds"
    )
