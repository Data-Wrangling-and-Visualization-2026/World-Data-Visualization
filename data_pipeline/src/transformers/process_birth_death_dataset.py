import os
import shutil

import numpy as np
import pandas as pd

from pathlib import Path
from dotenv import load_dotenv


load_dotenv()

# Root directory for structured CSV files (birth*/death*)
STRUCTURED_DIR = Path(os.getenv("OUTPUT_PATH"))

# Where the unified / cleaned datasets will be written
TRANSFORMED_DIR = STRUCTURED_DIR.parent / "transformed_data"


def _read_and_basic_cleanup(csv_path: Path) -> pd.DataFrame:
    """
    Read a CSV and perform initial structural cleanup:
    - Allows variable-length rows, trimming any trailing values
      beyond the header length.
    - Drops 'Index' column if present.
    """
    # First, read via csv module so we can trim rows to header length
    rows: list[list[str]] = []
    with csv_path.open(encoding="utf-8") as f:
        header_line = f.readline()
        header = [h.strip() for h in header_line.rstrip("\n\r").split(",")]
        n_cols = len(header)
        rows.append(header)

        for line in f:
            parts = [p for p in line.rstrip("\n\r").split(",")]
            if not parts or all(p == "" for p in parts):
                continue
            # Trim any trailing values beyond the header width
            if len(parts) > n_cols:
                parts = parts[:n_cols]
            rows.append(parts)

    df = pd.DataFrame(rows[1:], columns=rows[0])

    if "Index" in df.columns:
        df = df.drop(columns=["Index"])

    return df


def _collapse_extra_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove 'Extra Info' and sum the numeric values per country.
    NaNs are treated as zeros during summation.
    """
    if "Country" not in df.columns:
        raise ValueError("Expected a 'Country' column in dataset.")

    if "Extra Info" not in df.columns:
        # Still ensure numeric types are consistent
        numeric_cols = [c for c in df.columns if c != "Country"]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        return df

    df_no_extra = df.drop(columns=["Extra Info"])

    numeric_cols = [c for c in df_no_extra.columns if c != "Country"]
    df_no_extra[numeric_cols] = df_no_extra[numeric_cols].apply(
        pd.to_numeric, errors="coerce"
    ).fillna(0)

    grouped = (
        df_no_extra.groupby("Country", as_index=False)[numeric_cols]
        .sum(min_count=1)
    )

    return grouped


def _load_clean_file(csv_path: Path) -> pd.DataFrame:
    """Full cleaning step for a single birth/death CSV file."""
    df = _read_and_basic_cleanup(csv_path)
    df = _collapse_extra_info(df)
    return df


def _combine_year_slices(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine multiple per-period DataFrames (all with a 'Country' column and
    year columns) into a single wide table joined on 'Country'.
    """
    if not dfs:
        return pd.DataFrame()

    # Align on 'Country' via index joins
    combined = dfs[0].set_index("Country")
    for df in dfs[1:]:
        right = df.set_index("Country")
        # Use suffixes when there is column overlap, then collapse duplicates
        combined = combined.join(right, how="outer", rsuffix="_dup")

        # If any duplicate columns were created (e.g. overlapping years),
        # keep the non-NaN value across the pair and drop the *_dup column.
        dup_cols = [c for c in combined.columns if c.endswith("_dup")]
        for dup in dup_cols:
            base = dup[:-4]

            if base in combined.columns:
                combined[base] = combined[base].where(
                    combined[base].notna(), combined[dup]
                )
                combined = combined.drop(columns=[dup])

    combined = combined.reset_index()

    # Sort year columns numerically, keep 'Country' first
    cols = list(combined.columns)
    non_country = [c for c in cols if c != "Country"]

    def _year_key(col: str) -> float:
        try:
            return float(int(col))
        except (TypeError, ValueError):
            # Non-year columns (if any) go to the end in original order
            return float("inf")

    non_country_sorted = sorted(non_country, key=_year_key)
    ordered_cols = ["Country"] + non_country_sorted
    combined = combined[ordered_cols]

    return combined


def _clip_row_outliers(row: pd.Series) -> pd.Series:
    """
    Clip numeric values in a row to [Q1 - 1.5 * IQR, Q3 + 1.5 * IQR].
    NaNs are ignored when computing quantiles.
    """
    values = row.to_numpy(dtype=float)
    mask = ~np.isnan(values)

    if mask.sum() < 2:
        # Not enough data to define an IQR; leave row as-is
        return row

    q1, q3 = np.percentile(values[mask], [25, 75])
    iqr = q3 - q1

    if iqr == 0:
        lower, upper = q1, q3
    else:
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

    clipped = np.clip(values, lower, upper)
    return pd.Series(clipped, index=row.index)


def _add_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure that all intermediate years between the minimum and maximum
    observed year columns exist. Missing years are added as NaN columns
    so that they can be filled later via interpolation.
    """
    if df.empty:
        return df

    cols = [c for c in df.columns if c != "Country"]

    year_cols: list[int] = []
    for c in cols:
        try:
            year_cols.append(int(c))
        except (TypeError, ValueError):
            continue

    if not year_cols:
        return df

    min_year, max_year = min(year_cols), max(year_cols)
    full_years = list(range(min_year, max_year + 1))

    # Add missing year columns initialised with NaN
    for year in full_years:
        col = str(year)
        if col not in df.columns:
            df[col] = np.nan

    # Reorder columns: Country, then all years ascending, then any non-year extras
    year_cols_all = [str(y) for y in sorted(full_years)]
    extra_cols = [c for c in cols if not c.isdigit()]

    ordered = ["Country"] + year_cols_all + extra_cols
    return df[ordered]


def _postprocess_wide_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Clip outliers row-wise using the IQR rule.
    - Linearly interpolate NaNs across columns (years) per row.
    """
    if df.empty:
        return df

    # Insert missing intermediate years with NaNs so interpolation can fill them
    df = _add_years(df)
    df = df.where(df != 0, np.nan)

    numeric_cols = [c for c in df.columns if c != "Country"]

    # Ensure numeric dtype and allow NaNs
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    # Clip outliers row-wise
    df[numeric_cols] = df[numeric_cols].apply(
        _clip_row_outliers, axis=1, result_type="expand"
    )

    # Interpolate NaNs along the year axis for each country
    df[numeric_cols] = df[numeric_cols].interpolate(
        axis=1, limit_direction="both"
    )

    # If data for the country is completely absent, we need to drop it
    df = df[df.isna().sum(axis=1) == 0]

    return df


def copy_files(*filenames: str):
    for file in filenames:
        shutil.copy2(file, TRANSFORMED_DIR)


def main() -> None:
    try:
        TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)

        # Collect and clean all birth-related CSVs
        birth_dfs: list[pd.DataFrame] = []
        for csv_path in sorted(STRUCTURED_DIR.glob("birth*.csv")):
            birth_dfs.append(_load_clean_file(csv_path))

        # Collect and clean all death-related CSVs
        death_dfs: list[pd.DataFrame] = []
        for csv_path in sorted(STRUCTURED_DIR.glob("death*.csv")):
            death_dfs.append(_load_clean_file(csv_path))

        # Combine by joining years horizontally
        birth_combined = _combine_year_slices(birth_dfs)
        death_combined = _combine_year_slices(death_dfs)

        # Post-process: outlier clipping and interpolation
        birth_final = _postprocess_wide_dataset(birth_combined)
        death_final = _postprocess_wide_dataset(death_combined)

        # Save outputs
        if not birth_final.empty:
            birth_final.to_csv(TRANSFORMED_DIR / "birth_dataset.csv", index=False)

        if not death_final.empty:
            death_final.to_csv(TRANSFORMED_DIR / "death_dataset.csv", index=False)

    except Exception as e:
        print(f"❌ The wrangling of birth_dataset.csv and death_dataset.csv is failed: {e}")

    else:
        print(f"✅ The birth_dataset.csv and death_dataset.csv were cleansed and transformed successfully")




if __name__ == "__main__":
    main()

