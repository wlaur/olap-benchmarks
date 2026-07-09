from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from olap_benchmarks.suites.jsonbench.config import iter_jsonbench_input_lines, write_jsonbench_input_file


def _write_gzip(path: Path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(lines)


def test_jsonbench_input_lines_repair_split_records_and_preserve_row_count(tmp_path: Path) -> None:
    input_file = tmp_path / "file_0001.json.gz"
    _write_gzip(
        input_file,
        [
            '{"text":"a\\u0000b"}\n',
            '{"text":"first\n',
            'second","kind":"commit"}\n',
            '{"ok":true}\n',
        ],
    )

    lines = list(iter_jsonbench_input_lines(input_file))

    assert lines == [
        '{"text":"ab"}\n',
        '{"text":"first\\nsecond","kind":"commit"}\n',
        "{}\n",
        '{"ok":true}\n',
    ]
    assert len(lines) == 4
    assert [json.loads(line) for line in lines] == [
        {"text": "ab"},
        {"text": "first\nsecond", "kind": "commit"},
        {},
        {"ok": True},
    ]


def test_write_jsonbench_input_file_uses_normalized_lines(tmp_path: Path) -> None:
    input_file = tmp_path / "file_0001.json.gz"
    output_file = tmp_path / "file_0001.json"
    _write_gzip(input_file, ['{"text":"first\n', 'second"}\n'])

    write_jsonbench_input_file(input_file, output_file)

    assert output_file.read_text(encoding="utf-8") == '{"text":"first\\nsecond"}\n{}\n'


def test_jsonbench_input_lines_reject_unrepaired_fragments(tmp_path: Path) -> None:
    input_file = tmp_path / "file_0001.json.gz"
    _write_gzip(input_file, ["not-json\n"])

    with pytest.raises(RuntimeError, match="unrepaired fragment"):
        list(iter_jsonbench_input_lines(input_file))
