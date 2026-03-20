import re

from ..settings import REPO_ROOT

MINUTE_PRECISION_TIMESTAMP_LITERAL_RE = re.compile(
    r"'[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})?'"
)


def test_sql_timestamp_literals_include_seconds() -> None:
    offending_literals: list[str] = []

    for sql_file in (REPO_ROOT / "olap_benchmarks").rglob("*.sql"):
        for line_number, line in enumerate(sql_file.read_text().splitlines(), start=1):
            match = MINUTE_PRECISION_TIMESTAMP_LITERAL_RE.search(line)
            if match is None:
                continue

            offending_literals.append(f"{sql_file.relative_to(REPO_ROOT)}:{line_number}: {match.group(0)}")

    assert offending_literals == []
