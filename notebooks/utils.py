import polars as pl


def compare(df1: pl.DataFrame, df2: pl.DataFrame):
    print("✅ Checking DataFrames...")

    if "time" in df1.columns and "time" in df2.columns:
        df1 = df1.sort("time")
        df2 = df2.sort("time")

    if df1.equals(df2):
        print("✅ DataFrames are completely equal!")
        return

    print("❌ DataFrames are NOT equal.\n")

    if df1.columns != df2.columns:
        print("⚠️ Column name mismatch!")
        print(f"  df1: {df1.columns}")
        print(f"  df2: {df2.columns}")
        common_cols = [c for c in df1.columns if c in df2.columns]
        print(f"✅ Comparing only common columns: {common_cols}")
        df1 = df1.select(common_cols)
        df2 = df2.select(common_cols)
    else:
        print("✅ Column names match.")

    if df1.shape != df2.shape:
        print(f"⚠️ Shape mismatch: df1={df1.shape}, df2={df2.shape}")

    diff_cols = []
    for col in df1.columns:
        dtype = df1[col].dtype
        if dtype == pl.Object:
            print(f"⚠️ Skipping column '{col}' of dtype Object (cannot compare)")
            continue
        try:
            if not df1[col].equals(df2[col]):
                diff_cols.append(col)
        except Exception as e:
            print(f"⚠️ Error comparing column '{col}': {e}")
            continue

    if diff_cols:
        print()
        print(f"⚠️ Columns with differences: {diff_cols}")
    else:
        print("✅ All comparable columns are equal!")

    comparable_cols = [c for c in df1.columns if c not in diff_cols and df1[c].dtype != pl.Object]
    if comparable_cols:
        print(f"\n✅ No differences in {len(comparable_cols):_} columns")

    if diff_cols:
        print(f"\n🔎 Showing differing rows: {diff_cols}")
        df1_diff = df1.select(diff_cols)
        df2_diff = df2.select(diff_cols)

        try:
            diffs = df1_diff != df2_diff
        except Exception:
            print(f"Cannot compare dataframes\n{df1_diff=}\n{df2_diff=}")
            return

        row_mask = diffs.select(pl.any_horizontal(pl.all())).to_series().fill_null(False)
        if row_mask.sum() > 0:
            print(f"⚠️ Number of differing rows: {row_mask.sum():_}")
            comparison = (
                pl.concat(
                    [
                        df1_diff.select(pl.all().name.suffix("_1")),
                        df2_diff.select(pl.all().name.suffix("_2")),
                        row_mask.to_frame("mismatch"),
                    ],
                    how="horizontal",
                )
                .filter(pl.col.mismatch)
                .drop("mismatch")
            )

            print(comparison)
        else:
            print("✅ No differing rows found!")
