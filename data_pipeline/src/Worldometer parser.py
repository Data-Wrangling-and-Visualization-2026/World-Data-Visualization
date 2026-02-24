

import time
import re
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from typing import List, Dict


BASE_URL = "https://www.worldometers.info"
POPULATION_PAGE = urljoin(BASE_URL, "/population/")
CO2_PAGE = urljoin(BASE_URL, "/co2-emissions/")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; WorldometerScraper/1.0; +https://example.com/bot)"
}
DELAY_BETWEEN_REQUESTS = 1.0  # seconds


def fetch_page(url: str) -> str:
    """Return raw HTML of the given URL."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def clean_num(text: str):
    """Convert a string to int/float, stripping commas and non‑numeric chars."""
    if not text or text.lower() in {"n/a", "na", "-"}:
        return None
    text = text.replace(",", "").strip()
    text = re.sub(r"[^\d\.\-]", "", text)
    try:
        return float(text) if "." in text else int(text)
    except ValueError:
        return None



def parse_population_list(html: str) -> List[Dict]:
    """Extract list of countries with their population page URLs."""
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one(
        "body > div > div.container.min-h-\[500px\].grow.py-4 > div > div.col-span-3 > div.prose.max-w-none.text-sm\/5.md\:text-base\/6.prose-table\:text-sm\/5.md\:prose-table\:text-base\/6.prose-headings\:font-medium.prose-headings\:mt-10.prose-h1\:text-4xl.prose-h1\:font-medium.prose-h1\:mb-8.prose-h2\:text-3xl.prose-h3\:text-2xl.prose-tr\:border-0.prose-thead\:border-0.prose-img\:mb-0.prose-a\:font-inherit.prose-li\:my-0\.5.prose-h4\:text-lg.prose-h4\:font-bold > ul:nth-child(19)"
    )
    if not ul:
        raise RuntimeError("Не удалось найти список стран на странице населения.")
    countries = []
    for li in ul.find_all("li"):
        a = li.find("a")
        if a and a.get("href"):
            countries.append(
                {"name": a.get_text(strip=True), "url": urljoin(BASE_URL, a["href"])}
            )
    print("Парсинг списка стран по населению прошёл успешно.")
    return countries


def parse_population_tables(html: str, country: str) -> pd.DataFrame:
    """Parse the population table(s) from a country's page."""
    tables = pd.read_html(html, header=0)
    if len(tables) < 2:
        raise RuntimeError("Найдено меньше двух таблиц на странице населения.")
    df1, df2 = tables[0], tables[1]
    df1.columns = df1.columns.str.replace(r"_historical$", "", regex=True)
    df2.columns = df2.columns.str.replace(r"_forecast$", "", regex=True)
    merged = pd.concat([df1, df2], ignore_index=True, sort=False)
    if len(merged.columns) > 0:
        last_col = merged.columns[-1]
        merged.rename(columns={last_col: "Global Rank"}, inplace=True)
    merged.insert(0, "Country", country)
    if "Year" in merged.columns:
        merged = merged.sort_values(by=["Country", "Year"], ascending=[True, False])
    print(f"Парсинг населения страны {country} прошел успешно")
    return merged



def parse_co2_list(html: str) -> List[Dict]:
    """Extract list of countries with their CO₂ emissions page URLs."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one(
        "table"
    )
    if not table:
        raise RuntimeError("Не удалось найти таблицу со странами на странице CO₂.")
    links = []
    for a in table.select("td:nth-child(2) > a"):
        href = a.get("href")
        if href:
            links.append(
                {"country": a.get_text(strip=True), "url": urljoin(BASE_URL, href)}
            )
    print("Парсинг стран по эмиссии прошел успешно")
    return links


def parse_co2_tables(html: str, country: str) -> pd.DataFrame:
    tables = pd.read_html(html, header=0)
    if len(tables) == 0:
        raise RuntimeError("Не найдено таблиц на странице CO₂.")
    if len(tables) == 1:
        df = tables[0]
    else:
        df1, df2 = tables[0], tables[1]
        df1.columns = df1.columns.str.replace(r"_historical$", "", regex=True)
        df2.columns = df2.columns.str.replace(r"_forecast$", "", regex=True)
        df = pd.concat([df1, df2], ignore_index=True, sort=False)

    df.columns = df.columns.str.replace(r"_historical$", "", regex=True)
    df.columns = df.columns.str.replace(r"_forecast$", "", regex=True)
    if len(df.columns) > 0:
        last_col = df.columns[-1]
    df.drop(['Population', 'Pop. change'], axis=1)

    df.insert(0, "Country", country)
    if "Year" in df.columns:
        df = df.sort_values(by=["Country", "Year"], ascending=[True, False])
    print(df.head(18))
    print(f"Парсинг эмиссии страны {country} прошёл успешно")
    return df



def main():


    print("\nСкачиваем страницу CO₂‑эмиссий…")
    co2_html = fetch_page(CO2_PAGE)

    print("🔎 Парсим список стран…")
    co2_countries = parse_co2_list(co2_html)
    print(f"✅ Найдено {len(co2_countries)} стран для CO₂.")

    co2_data = []
    for idx, entry in enumerate(co2_countries, start=1):
        name, url = entry["country"], entry["url"]
        print(f"[{idx}/{len(co2_countries)}] Обрабатываем {name}…")
        try:
            country_html = fetch_page(url)
            df = parse_co2_tables(country_html, name)
            df["CO2 URL"] = url
            co2_data.append(df)
        except Exception as e:
            print(f"Ошибка при обработке {name}: {e}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if not co2_data:
        print("Никакие данные по CO₂ не собраны.")
        return

    co2_df = pd.concat(co2_data, ignore_index=True)
    #co2_df = co2_df.apply(pd.to_numeric, errors="coerce")
    print("Объединяем все данные по CO₂…")

    print("Скачиваем страницу населения…")
    pop_html = fetch_page(POPULATION_PAGE)

    print("Парсим список стран…")
    pop_countries = parse_population_list(pop_html)
    print(f"✅ Найдено {len(pop_countries)} стран для населения.")

    pop_data = []
    for idx, entry in enumerate(pop_countries, start=1):
        name, url = entry["name"], entry["url"]
        print(f"[{idx}/{len(pop_countries)}] Обрабатываем {name}…")
        try:
            country_html = fetch_page(url)
            df = parse_population_tables(country_html, name)
            df["Population URL"] = url
            pop_data.append(df)
        except Exception as e:
            print(f"Ошибка при обработке {name}: {e}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if not pop_data:
        print("Никакие данные по населению не собраны.")
        return

    pop_df = pd.concat(pop_data, ignore_index=True)
    #pop_df = pop_df.apply(pd.to_numeric, errors="coerce")
    print("Объединяем все данные по населению…")

    print("\nОбъединяем население и CO₂ по странам…")
    merged_df = pd.merge(pop_df, co2_df, on="Country", how="outer", suffixes=("_pop", "_co2"))

    output_file = "worldometer_population_and_co2_data.csv"
    merged_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"Данные сохранены в {output_file}")
    print("\nПервые 5 строк объединённого DataFrame:")
    print(merged_df.head())


if __name__ == "__main__":
    main()