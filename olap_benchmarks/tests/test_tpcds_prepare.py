from __future__ import annotations

import subprocess
from pathlib import Path
from typing import NoReturn

import pytest

from olap_benchmarks.suites.tpc_ds import config as tpcds_config


def fake_tpcgen_path(name: str) -> str:
    return f"/bin/{name}"


def one_free_byte(path: Path) -> int:
    _ = path
    return 1


def test_tpcds_prepare_refuses_low_free_space(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_run(command: list[str], check: bool) -> NoReturn:
        _ = command, check
        raise AssertionError("subprocess.run should not be called")

    monkeypatch.setattr(tpcds_config.SETTINGS, "input_data_directory", tmp_path)
    monkeypatch.setattr(tpcds_config.shutil, "which", fake_tpcgen_path)
    monkeypatch.setattr(tpcds_config, "_free_disk_bytes", one_free_byte)
    monkeypatch.setattr(tpcds_config.subprocess, "run", fail_run)

    with pytest.raises(RuntimeError, match="Refusing to generate tpc_ds_sf1 data"):
        tpcds_config._prepare_tpc_ds_data(1)


def test_tpcds_prepare_rejects_partial_dataset_with_generator_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    input_dir = tmp_path / "tpc_ds_sf1"
    input_dir.mkdir()
    (input_dir / "dbgen_version.parquet").write_bytes(b"partial")

    monkeypatch.setattr(tpcds_config.SETTINGS, "input_data_directory", tmp_path)

    with pytest.raises(ValueError, match="partial dataset"):
        tpcds_config._prepare_tpc_ds_data(1)


def test_tpcds_normalize_removes_tmp_file_after_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source = tmp_path / "income_band.parquet"
    source.write_bytes(b"source")
    tmp_file = tmp_path / "income_band.parquet.tmp"

    class FakeScan:
        def collect_schema(self) -> tpcds_config.pl.Schema:
            return tpcds_config.pl.Schema({"ib_income_band_id": tpcds_config.pl.Int64})

        def select(self, exprs: list[tpcds_config.pl.Expr]) -> FakeScan:
            _ = exprs
            return self

        def sink_parquet(self, path: Path) -> None:
            path.write_bytes(b"tmp")
            raise subprocess.CalledProcessError(returncode=1, cmd="sink_parquet")

    def fake_scan_parquet(path: Path) -> FakeScan:
        _ = path
        return FakeScan()

    def enough_free_bytes(path: Path) -> int:
        _ = path
        return tpcds_config.TPCDS_PREPARE_FREE_SPACE_RESERVE_BYTES + 100

    monkeypatch.setattr(tpcds_config, "TPCDS_TABLES", ("income_band",))
    monkeypatch.setattr(tpcds_config.pl, "scan_parquet", fake_scan_parquet)
    monkeypatch.setattr(tpcds_config, "_free_disk_bytes", enough_free_bytes)

    with pytest.raises(subprocess.CalledProcessError):
        tpcds_config._normalize_tpc_ds_parquet(tmp_path)

    assert not tmp_file.exists()


def test_tpcds_is_normalized_checks_all_renamed_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    schemas = {
        "income_band": tpcds_config.pl.Schema({"ib_income_band_sk": tpcds_config.pl.Int64}),
        "reason": tpcds_config.pl.Schema({"r_reason_description": tpcds_config.pl.String}),
    }

    class FakeScan:
        def __init__(self, schema: tpcds_config.pl.Schema) -> None:
            self.schema = schema

        def collect_schema(self) -> tpcds_config.pl.Schema:
            return self.schema

    def fake_scan_parquet(path: Path) -> FakeScan:
        return FakeScan(schemas[path.stem])

    monkeypatch.setattr(tpcds_config, "TPCDS_TABLES", ("income_band", "reason"))
    monkeypatch.setattr(tpcds_config.pl, "scan_parquet", fake_scan_parquet)

    assert tpcds_config._is_normalized(tmp_path) is False


def test_tpcds_is_normalized_checks_decimal_widths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeScan:
        def collect_schema(self) -> tpcds_config.pl.Schema:
            return tpcds_config.pl.Schema({"ss_net_paid": tpcds_config.pl.Decimal(38, 2)})

    def fake_scan_parquet(path: Path) -> FakeScan:
        _ = path
        return FakeScan()

    monkeypatch.setattr(tpcds_config, "TPCDS_TABLES", ("store_sales",))
    monkeypatch.setattr(tpcds_config.pl, "scan_parquet", fake_scan_parquet)

    assert tpcds_config._is_normalized(tmp_path) is False


def test_tpcds_normalization_accepts_tpcgen_decimal_width() -> None:
    exprs = tpcds_config._normalization_exprs(
        tpcds_config.pl.Schema({"ss_net_paid": tpcds_config.pl.Decimal(38, 2)}),
        "store_sales",
    )

    assert len(exprs) == 1


def test_tpcds_normalization_rejects_unexpected_decimal_width() -> None:
    with pytest.raises(ValueError, match="Unexpected TPC-DS decimal width for ss_net_paid"):
        tpcds_config._normalization_exprs(
            tpcds_config.pl.Schema({"ss_net_paid": tpcds_config.pl.Decimal(12, 2)}),
            "store_sales",
        )
