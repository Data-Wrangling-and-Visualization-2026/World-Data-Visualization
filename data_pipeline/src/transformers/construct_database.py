import os
import sqlite3

import pandas as pd

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = os.getenv("PROJECT_PATH") + "/data_pipeline"
TRANSFORMED_DIR = BASE_DIR + "/transformed_data"
DB_DIR = os.getenv("DATABASE_PATH")
DB_PATH = DB_DIR + "countries_stats.db"


def _is_year_column(name: str) -> bool:
    """Return True if a column name looks like a year (4-digit int)."""
    try:
        year = int(name)
        return 1940 <= year <= 2030
    except (TypeError, ValueError):
        return False


def _load_birth_or_death_long(csv_path: str, kind: str) -> pd.DataFrame:
    """
    Load birth_dataset.csv or death_dataset.csv and melt year columns to long format.
    `kind` should be 'birth' or 'death' and is used to name the value column.
    """
    df = pd.read_csv(csv_path)

    if "Country" not in df.columns:
        raise ValueError(f"'Country' column is required in {os.path.basename(csv_path)}")

    year_cols = [c for c in df.columns if c != "Country" and _is_year_column(c)]

    if not year_cols:
        raise ValueError(f"No year columns found in {os.path.basename(csv_path)}")

    long_df = df.melt(
        id_vars=["Country"],
        value_vars=year_cols,
        var_name="Year",
        value_name=f"{kind.capitalize()}",
    )

    long_df["Year"] = pd.to_numeric(long_df["Year"], errors="coerce").astype("Int64")
    long_df = long_df.dropna(subset=["Year"])
    long_df["Year"] = long_df["Year"].astype(int)

    long_df["Country"] = long_df["Country"].astype(str)

    return long_df


def _load_transformed_tables() -> list[pd.DataFrame]:
    """
    Load and normalize all CSVs from transformed_data:
    - birth_dataset.csv and death_dataset.csv are melted to long format.
    - Other worldometer_* files are left long-form but non-key columns are prefixed.
    """
    tables: list[pd.DataFrame] = []

    if not os.path.exists(TRANSFORMED_DIR):
        return tables

    for filename in sorted(os.listdir(TRANSFORMED_DIR)):
        if not filename.endswith(".csv"):
            continue

        filepath = TRANSFORMED_DIR + f"/{filename}"

        if filename == "birth_dataset.csv":
            tables.append(_load_birth_or_death_long(filepath, kind="birth"))

        elif filename == "death_dataset.csv":
            tables.append(_load_birth_or_death_long(filepath, kind="death"))

        else:
            df = pd.read_csv(filepath)
            tables.append(df)

    return tables


def _merge_on_year_country(tables: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Inner join all tables on ['Country', 'Year'].
    """
    if not tables:
        return pd.DataFrame()

    merged = tables[0]

    for df in tables[1:]:
        dups = set(df.columns) & set(merged.columns) - {"Country", "Year"}

        for dup in dups:
            df.drop(dup, axis=1, inplace=True)

        merged = merged.merge(df, on=["Country", "Year"], how="inner")

    # Ensure consistent ordering of columns: Year, Country, then the rest
    cols = list(merged.columns)
    other_cols = [c for c in cols if c not in ("Year", "Country")]
    merged = merged[["Country", "Year"] + other_cols]

    return merged


def _write_sqlite(df: pd.DataFrame) -> None:
    """
    Create SQLite database and write the 'country' table with:
    - PRIMARY KEY (Year, Country)
    - Year: INTEGER, Country: TEXT, others: REAL
    """
    if df.empty:
        return

    os.makedirs(name=DB_DIR, exist_ok=True)

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    # Enforce dtypes
    df["Country"] = df["Country"].astype(str)
    df["Year"] = pd.to_numeric(df["Year"], errors="coerce").astype(int)

    other_cols = [c for c in df.columns if c not in ("Year", "Country")]

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS country")

    column_defs = [
        "Year INTEGER",
        "Country TEXT",
    ] + [f'"{c}" REAL' if " " in c else f"{c} REAL" for c in other_cols]

    create_sql = (
        "CREATE TABLE country ("
        + ", ".join(column_defs)
        + ", PRIMARY KEY (Year, Country)"
        + ");"
    )
    cur.execute(create_sql)

    column_table_names = [f'"{c}"' if " " in c else c for c in df.columns]

    placeholders = ", ".join(["?"] * len(df.columns))
    insert_sql = f'INSERT INTO country ({", ".join(column_table_names)}) VALUES ({placeholders})'

    cur.executemany(insert_sql, df.itertuples(index=False, name=None))

    conn.commit()
    conn.close()


def main() -> None:
    tables = _load_transformed_tables()
    merged = _merge_on_year_country(tables)
    _write_sqlite(merged)


if __name__ == "__main__":
    main()
