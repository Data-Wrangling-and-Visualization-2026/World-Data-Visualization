#!/usr/bin/env python3
"""
Batch PDF Processing for Multiple Data Sources
Handles Energy Inst, WorldBank, and UN PDFs
"""

from pathlib import Path
from pdf_to_csv import PDFToCSVExtractor

import json
import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def batch_process_pdfs():
    """Process all PDFs from different sources with page pre-extraction."""
    # Check API Key
    if not os.getenv("GOOGLE_API_KEY"):
        print("❌ GOOGLE_API_KEY not set in environment.")
        print("💡 Please set GOOGLE_API_KEY in data_pipeline/.env")
        return

    extractor = PDFToCSVExtractor(cleanup_temp=True)

    # Define PDF sources from project proposal
    # Adjust page numbers based on actual PDF content
    pdf_configs = [
        {
            "pdf_path": "data_pipeline/raw_data/energy_inst.pdf",
            "output_filename": "energy_statistics",
            "pages": [0, 1, 2, 3],
            "table_description": "Oil, Gas, Coal production by country and year"
        },
        {
            "pdf_path": "data_pipeline/raw_data/worldbank_water.pdf",
            "output_filename": "water_resources",
            "pages": [0, 1, 2],
            "table_description": "Renewable water resources statistics by country"
        },
        {
            "path": "data_pipeline/raw_data/un_demographics.pdf",
            "output_filename": "demographics",
            "pages": [0, 1, 2, 3, 4],
            "table_description": "Death, Birth, Sex Ratio statistics by country"
        }
    ]

    birth_rate_1950s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1950_1959_DYB.pdf",
        "output_filename": "birth1950",
        "pages": [214 + i for i in range(0, 16, 2)],
        "table_description": """
Birth statsitics by country as indices and years 1949-1958 as headers.

Ignore the following information:
— Continent
— Code field
— The "*" symbol near the numerical value if presents

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"

A country may contain subfields, for example,
```
Algeria:
├─ European Population
└─ Moslem Population
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1949,1950,1951,1952,1953,1954,1955,1956,1957,1958
Algeria,European population,21033,20512,20375,20145,19162,19760,19575,19230,19892,20779
Algeria,Moslem population,259634,306808,324171,339818,343100,362900,379546,297848,272693,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"
"""
    }

    birth_rate_1960s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1960_1969_DYB_2.pdf",
        "output_filename": "birth1960s",
        "pages": [252 + i for i in range(0, 10)],
        "table_description": """
Birth statsitics by country as indices and years as headers. Note that each page with 1950-1959 years data alternates
by a page with 1960-1969 years data for the same country. For avoiding confusion, the page with 1960-1969 years data
contains the French name of the country as the last column. You must accurately join the 1950-1959 years columns
with 1960-1969 years columns into one row.

Do not include in the CSV the following:
— 1950-1958 years fields inclusively
— Continent
— Code field
— The noisy symbols near the numerical value if present
— The column with French names of the countries

Correct the country names and remove redundant numbers and symbols, for example:
"Algeria 2" -> "Algeria"
"Non-African population S•••••••••••" -> "Non-African population"

A country may contain subfields, for example,
```
Algeria:
├─ Algerian Population
└─ Non-Algerian Population
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1959,1960,1961,1962,1963,1964,1965,1966,1967,1968,1969
Algeria,Algerian Population,306808,324171,339818,343100,362900,379546,297848,286982,322748,374519
Algeria,Non-Algerian Population,20512,20375,20145,19162,19760,19575,19230,19892,20961,22819
```

If there is no subfield for the country, fill the "Extra Info" with "nan"

Be careful and validate the proper CSV format (e.g. avoid the column inconsistencies in different rows)"""}

    birth_rate_1970_1974s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1970_1979_1_DYB.pdf",
        "output_filename": "birth1970_1974s",
        "pages": [260 + i for i in range(0, 5)],
        "table_description": '''
        Birth statsitics by country as indices and years 1970-1974 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country may contain subfields, for example,
```
Greece - Grece:
├─ Urban - Urbaine
├─ Semi-urban - Semi-urbaine
└─ Rural - Rurale
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1970,1971,1972,1973,1974
Greece,Urban,77979,77754,80614,80389,nan
Greece,Semi-urban,16726,16006,nan,nan,nan
Greece,Rural,50223,47366,60277,57137,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"'''}

    birth_rate_1975_1979s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1970_1979_2_DYB.pdf",
        "output_filename": "birth1975_1979s",
        "pages": [306 + i for i in range(0, 7)],
        "table_description": '''
        Birth statsitics by country as indices and years 1975-1979 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country may contain subfields, for example,
```
Greece - Grece:
├─ Urban - Urbaine
├─ Semi-urban - Semi-urbaine
└─ Rural - Rurale
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1970,1971,1972,1973,1974
Greece,Urban,77979,77754,80614,80389,nan
Greece,Semi-urban,16726,16006,nan,nan,nan
Greece,Rural,50223,47366,60277,57137,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"'''}

    birth_rate_1980_1985s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1980_1985_DYB.pdf",
        "output_filename": "birth1980_1985s",
        "pages": [300 + i for i in range(0, 5)],
        "table_description": '''
        Birth statsitics by country as indices and years 1975-1979 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— Rate-Taux fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"
"Bot~wana" -> "Botswana"

A country may contain subfields, for example,
```
USSR - URSS:
├─ USSR - URSS
├─ Byelorussion SSR - RSS de Bielorussie
└─ Ukrainian SSR - RSS d'Ukroine
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1981,1982,1983,1984,1985
USSR,USSR,4961363,5100282,5391869,5386893
USSR,Byelorussion SSR,157899,159364,173510,168749,nan
USSR,Ukrainian SSR,733183,745591,807111,790800,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"

Do not confuse the subfield with the French name of the country'''}

    birth_rate_1985_1989s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1985_1989_DYB.pdf",
        "output_filename": "birth1985_1989s",
        "pages": [297 + i for i in range(0, 5)],
        "table_description": '''
        Birth statsitics by country as indices and years 1985-1989 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— Rate-Taux fields (for your convenience, if the number is float, then it is definitely a rate)
— 1985 year column

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"
"Bot~wana" -> "Botswana"

A country may contain subfields, for example,
```
USSR - URSS:
├─ USSR - URSS
├─ Byelorussion SSR - RSS de Bielorussie
└─ Ukrainian SSR - RSS d'Ukroine
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1986,1987,1988,1989
USSR,USSR,5610769,5599195,5381056,nan
USSR,Byelorussion SSR,171611,162937,163193,nan
USSR,Ukrainian SSR,792574,760851,744056,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"

Do not confuse the subfield with the French name of the country'''}

    birth_rate_1990_1995s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1990_1995_DYB.pdf",
        "output_filename": "birth1990_1995s",
        "pages": [333 + i for i in range(0, 5)],
        "table_description": '''
        Birth statsitics by country as indices and years 1991-1995 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— Rate-Taux fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"
"Bot~wana" -> "Botswana"

A country may contain subfields, for example,
```
USSR - URSS:
├─ USSR - URSS
├─ Byelorussion SSR - RSS de Bielorussie
└─ Ukrainian SSR - RSS d'Ukroine
```

Create an additional column "Extra Info" and fill with 'nan',

```
Country,Extra Info,1991,1992,1993,1994,1995
Egypt,nan,1636551,1496866,1644247,1719971,nan
```
'''}

    birth_rate_1995_1999s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1995_1999_DYB.pdf",
        "output_filename": "birth1995_1999s",
        "pages": [301 + i for i in range(0, 10)],
        "table_description": '''
        Birth statsitics by country as indices and years 1995-1999 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,1995,1996,1997,1998,1999
Georgia,Total,9869,9638,9275,8879,8435
```'''}

    birth_rate_2000_2005s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/2001_2005_DYB.pdf",
        "output_filename": "birth2000_2005s",
        "pages": [337 + i for i in range(0, 9)],
        "table_description": '''
        Birth statsitics by country as indices and years 2001-2005 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,2001,2002,2003,2004,2005
Georgia,Total,47589,46605,46194,49572,46512
```'''}

    birth_rate_2006_2010s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/2006_2010_DYB.pdf",
        "output_filename": "birth2006_2010s",
        "pages": [398 + i for i in range(0, 9)],
        "table_description": '''
        Birth statsitics by country as indices and years 2006-2010 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,2006,2007,2008,2009,2010
Georgia,Total,47795,49287,56565,nan,62 585
```'''}

    birth_rate_2010_2015s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/2011_2015_DYB.pdf",
        "output_filename": "birth2011_2015s",
        "pages": [363 + i for i in range(0, 9)],
        "table_description": '''
        Birth statsitics by country as indices and years 2011-2015 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,2011,2012,2013,2014,2015
Georgia,Total,58 014,57 031,57 878,60 635,nan
```'''}

    birth_rate_2016_2020s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/2016_2020_DYB.pdf",
        "output_filename": "birth2016_2020s",
        "pages": [380 + i for i in range(0, 9)],
        "table_description": '''
Birth statsitics by country as indices and years 2016-2020 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— RATE - TAUX fields (for your convenience, if the number is float, then it is definitely a rate)

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,2016,2017,2018,2019,2020
Georgia,Total,56 569,53 293,51 138,48 296,46 520
```'''}

    birth_rate_2020_2024s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/2021_2024_DYB.pdf",
        "output_filename": "birth2020_2024s",
        "pages": [385 + i for i in range(0, 9)],
        "table_description": '''
        Birth statsitics by country as indices and years 2020-2024 as headers.

Ignore the following information:
— French name of the country
— Continent
— Code field
— The "*" symbol near the numerical value if presents
— Rate - Taux fields (for your convenience, if the number is float, then it is definitely a rate)
— 2020 year column

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"
"....... Singapore 35 - Singapour 35 •.•." -> "Singapore"

A country contains subfields, like
```
Georgia - Georgie:
├─ Total
├─ Urban - Urbaine
└─ Rural - Rurale
```

Consider only Total Natality and create an additional column "Extra Info", filling it with "Total", for example,

```
Country,Extra Info,2021,2022,2023,2024
Georgia,Total,45 946,42 319,40 214,39 483
```'''}

    death_rate_1950s = {
        "pdf_path": f"{os.getenv('RAW_DATA_PATH')}/demographic_yearbooks/1950_1959_DYB.pdf",
        "output_filename": "death1950",
        "pages": [449 + i for i in range(0, 12, 2)],
        "table_description": '''
Death statsitics by country as indices and years 1949-1958 as headers.

Ignore the following information:
— Continent
— Code field
— The "*" symbol near the numerical value if presents

Correct the country names and remove redundant numbers and symbols, for example:
"Angola 2" -> "Angola"
"BathurstХХХХХХХХХХХХХХХХХХХ.Х" -> "Bathurst"

A country may contain subfields, for example,
```
Algeria:
├─ European Population
└─ Moslem Population
```

Therefore, create an additional column "Extra Info" with subfield names, for example,

```
Country,Extra Info,1949,1950,1951,1952,1953,1954,1955,1956,1957,1958
Algeria,European population,21033,20512,20375,20145,19162,19760,19575,19230,19892,20779
Algeria,Moslem population,259634,306808,324171,339818,343100,362900,379546,297848,272693,nan
```

If there is no subfield for the country, fill the "Extra Info" with "nan"'''}

    # Process PDFs
    results = extractor.process_pdf(
        pdf_path=death_rate_1950s["pdf_path"],
        output_filename=death_rate_1950s["output_filename"],
        pages=death_rate_1950s["pages"],
        table_description=death_rate_1950s["table_description"]
    )

    # Save results summary
    summary_path = Path(f"{os.getenv('PROJECT_PATH')}/data_pipeline/processing_summary.json")
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n📄 Processing summary saved to: {summary_path}")

    return results


if __name__ == "__main__":
    batch_process_pdfs()