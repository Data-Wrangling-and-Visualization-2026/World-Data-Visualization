from __future__ import annotations
import os
import re

from pathlib import Path
from dotenv import load_dotenv

import numpy as np
import pandas as pd

load_dotenv()


# Root directory for structured CSV files
PARSED_DIR = Path(os.getenv("OUTPUT_PATH"))

# Where the unified / cleaned datasets will be written
TRANSFORMED_DIR = PARSED_DIR.parent / "transformed_data"


def _extract_numeric_token(value: str) -> str:
    """
    From strings like 'â0.39%' or 'a 0.39%' keep only the first numeric token
    matching ([0-9,.]+%?). If nothing matches, return the original string.
    """
    if not isinstance(value, str):
        return value

    match = re.search(r"\"?(â)?([0-9,.]+)%?\"?", value)
    if match:
        return match.group(2)
    return value


def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply numeric-token extraction to all object (string-like) columns."""
    text_cols = df.select_dtypes(include=["object"]).columns

    for col in text_cols:
        if col == "Country":
            continue

        df[col] = df[col].map(_extract_numeric_token)

    return df


def _drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop URL columns and 'Global Rank' if present."""
    to_drop = [c for c in df.columns if c.endswith("URL") or c == "Global Rank"]
    if to_drop:
        df = df.drop(columns=to_drop)
    return df


def _add_missing_years_and_interpolate(
    df: pd.DataFrame,
    *,
    country_col: str = "Country",
    year_col: str = "Year",
) -> pd.DataFrame:
    """
    For each country:
    - Ensure all intermediate years between min and max year exist.
    - Interpolate numeric values across years.
    """
    if df.empty:
        return df

    # Ensure correct dtypes
    df[year_col] = pd.to_numeric(df[year_col], errors="coerce")

    numeric_cols = [
        c for c in df.columns if c not in (country_col, year_col)
    ]
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")

    pieces: list[pd.DataFrame] = []

    for country, g in df.groupby(country_col):
        g = g.sort_values(year_col)
        years = g[year_col].dropna().astype(int)
        if years.empty:
            pieces.append(g)
            continue

        full_index = np.arange(years.min(), years.max() + 1)

        g = g.set_index(year_col).reindex(full_index)
        g.index.name = year_col

        # Reattach country label
        g[country_col] = country

        # Interpolate numeric columns along year index
        g[numeric_cols] = g[numeric_cols].interpolate(
            axis=0, limit_direction="both"
        )

        pieces.append(g.reset_index())

    return pd.concat(pieces, ignore_index=True)


def _process_worldometer_file(csv_path: Path) -> None:
    """Clean and transform a single Worldometer CSV file."""
    df = pd.read_csv(csv_path)

    # 1) Clean textual numeric patterns
    df = _clean_text_columns(df)

    # 2) Drop URL / Global Rank columns
    df = _drop_unused_columns(df)

    # 3) Remove rows with year > 2025 (if Year column exists)
    if "Year" in df.columns:
        df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
        df = df[df["Year"] <= 2025]

    # 4) Add missing years per country and interpolate numeric values
    if {"Country", "Year"}.issubset(df.columns):
        df = _add_missing_years_and_interpolate(df, country_col="Country", year_col="Year")

    TRANSFORMED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = TRANSFORMED_DIR / csv_path.name
    df.to_csv(out_path, index=False)


def main() -> None:
    """
    Process all Worldometer-style CSVs in parsed_data.
    Functions are written generically and are not tied to specific filenames.
    """
    if not PARSED_DIR.exists():
        return

    for csv_path in sorted(PARSED_DIR.glob("worldometer_*.csv")):
        _process_worldometer_file(csv_path)


if __name__ == "__main__":
    main()

