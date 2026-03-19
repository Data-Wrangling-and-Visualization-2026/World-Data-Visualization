import os
import csv
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

STRUCTURED_DIR = Path(os.getenv("OUTPUT_PATH"))


# Mapping from various historical / alternative names to the
# modern country names used in the Worldometer CSVs.
# Keys are stored in lowercase; lookup is done case-insensitively.
MANUAL_NAME_MAP: dict[str, str] = {
    # Example mappings explicitly mentioned in the task
    "congo(brazzaville)": "Congo",
    "congo (brazzaville)": "Congo",
    "ussr": "Russia",
    "union of soviet socialist republics": "Russia",
    "byelorussian ssr": "Belarus",
    "ukrainian ssr": "Ukraine",

    # Congo / DR Congo and predecessors
    "belgian congo": "DR Congo",
    "zaire": "DR Congo",
    "congo (democratic republic of)": "DR Congo",
    "democratic republic of the congo": "DR Congo",

    # Colonial / historical African names
    "bechuanaland": "Botswana",
    "union of south africa": "South Africa",
    "south west africa": "Namibia",
    "nyasaland": "Malawi",
    "tanganyika": "Tanzania",
    "upper volta": "Burkina Faso",
    "dahomey": "Benin",
    "french somaliland": "Djibouti",
    "french territory of the afars and the issas": "Djibouti",
    "ceylon": "Sri Lanka",
    "malagasy republic": "Madagascar",
    "ruanda-urundi": "Rwanda",  # aggregated, closest single modern state
    "southern rhodesia": "Zimbabwe",
    "rhodesia and nyasaland fed. of": "Zimbabwe",
    "northern rhodesia": "Zambia",
    "zanzibar and pemba": "Tanzania",

    # Name updates (official renamings)
    "burma": "Myanmar",
    "myanmar": "Myanmar",
    "swaziland": "Eswatini",
    "eswatini": "Eswatini",
    "cabo verde": "Cabo Verde",
    "cape verde": "Cabo Verde",
    "cape verde islands": "Cabo Verde",
    "bahama islands": "Bahamas",
    "surinam": "Suriname",
    "libyan arab jamahiriya": "Libya",
    "libyan arab republic": "Libya",
    "lao people's dem. rep.": "Laos",
    "lao people's democratic republic": "Laos",
    "laos": "Laos",
    "viet nam": "Vietnam",
    "viet-nam": "Vietnam",
    "vietnam": "Vietnam",
    "viet-nam rep. of": "Vietnam",
    "viet-nam republic of": "Vietnam",

    # Korea variants
    "korea republic of": "South Korea",
    "republic of korea": "South Korea",
    "korea (republic of)": "South Korea",
    "south korea": "South Korea",
    "korea (democratic people's republic of)": "North Korea",
    "korea dem. people's rep.": "North Korea",
    "north korea": "North Korea",

    # Ivory Coast / Côte d'Ivoire
    "ivory coast": "Côte d'Ivoire",
    "cote d'ivoire": "Côte d'Ivoire",
    "côte d'ivoire": "Côte d'Ivoire",

    # Moldova
    "republic of moldova": "Moldova",

    # Russia / Russian Federation
    "russian federation": "Russia",

    # Miscellaneous normalizations where Worldometer uses one variant
    "netherlands (kingdom of the)": "Netherlands",
    "holy see": "Vatican City",
    "state of palestine": "Palestine",
    "occupied palestinian territory": "Palestine",
}


def normalize_key(name: str) -> str:
    """Normalize a country/territory name for dictionary lookup."""
    return name.strip().lower()


def standardize_name(name: str) -> str:
    """
    Return a standardized modern country name.
    If no mapping is defined, the original name is returned unchanged.
    """
    key = normalize_key(name)
    if not key:
        return name
    if key in MANUAL_NAME_MAP:
        return MANUAL_NAME_MAP[key]
    return name.strip()


def process_file(path: Path) -> None:
    """
    Read a birth*/death* CSV file, replace country names in the first column
    using the mapping above, and write a sibling *UPD.csv file.
    The row order and all non-name columns are preserved.
    """
    output_path = path.with_name(path.stem + "UPD.csv")

    with path.open(newline="", encoding="utf-8") as src, output_path.open(
        "w", newline="", encoding="utf-8"
    ) as dst:
        reader = csv.reader(src)
        writer = csv.writer(dst)

        header = next(reader, None)
        if header is not None:
            writer.writerow(header)

        for row in reader:
            if not row:
                writer.writerow(row)
                continue
            row = list(row)
            row[0] = standardize_name(row[0])
            writer.writerow(row)


def main() -> None:
    # Process all birth*.csv and death*.csv that are original files
    try:
        for pattern in ("birth*.csv", "death*.csv"):
            for csv_path in sorted(STRUCTURED_DIR.glob(pattern)):
                # Skip already-updated outputs if they exist
                if csv_path.name.endswith("UPD.csv"):
                    continue
                process_file(csv_path)

    except Exception as e:
        print(f"❌ The standardization of countries names went wrong: {e}")

    else:
        print(f"✅ The countries names were standardized to suit each other")



if __name__ == "__main__":
    main()

