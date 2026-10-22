from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa

DIRECTORY = Path(__file__).parent
UPSTREAM_COMMIT = "a39603662e023e449cb2121997a5034df9e02ebf"
_COLUMN = re.compile(r"(\w+) (integer|text|character varying\((\d+)\))( NOT NULL)?( PRIMARY KEY)?")


@dataclass(frozen=True, slots=True)
class Column:
    name: str
    sql_type: str
    nullable: bool
    max_characters: int | None

    @property
    def arrow_type(self) -> pa.DataType:
        return pa.int32() if self.sql_type == "integer" else pa.string()


def _load_schema() -> dict[str, tuple[Column, ...]]:
    tables: dict[str, tuple[Column, ...]] = {}
    for statement in (DIRECTORY / "upstream/schema.sql").read_text().split(";"):
        if not statement.strip():
            continue
        table = re.fullmatch(r"\s*CREATE TABLE (\w+) \((.*?)\)\s*", statement, re.DOTALL)
        if table is None:
            raise ValueError("Unexpected pinned JOB table definition")
        columns: list[Column] = []
        for definition in table[2].split(","):
            match = _COLUMN.fullmatch(definition.strip())
            if match is None:
                raise ValueError(f"Unexpected pinned JOB column: {definition}")
            columns.append(Column(match[1], match[2], match[4] is None, int(match[3]) if match[3] else None))
        tables[table[1]] = tuple(columns)
    return tables


JOB_SCHEMA = _load_schema()
JOB_TABLES = tuple(JOB_SCHEMA)
JOB_QUERY_NAMES = tuple(
    f"{number}{variant}"
    for number, variants in enumerate(
        (
            "abcd",
            "abcd",
            "abc",
            "abc",
            "abc",
            "abcdef",
            "abc",
            "abcd",
            "abcd",
            "abc",
            "abcd",
            "abc",
            "abcd",
            "abc",
            "abcd",
            "abcd",
            "abcdef",
            "abc",
            "abcd",
            "abc",
            "abc",
            "abcd",
            "abc",
            "ab",
            "abc",
            "abc",
            "abc",
            "abc",
            "abc",
            "abc",
            "abc",
            "ab",
            "abc",
        ),
        start=1,
    )
    for variant in variants
)


def arrow_schema(table: str) -> pa.Schema:
    return pa.schema(
        [pa.field(column.name, column.arrow_type, nullable=column.nullable) for column in JOB_SCHEMA[table]]
    )
