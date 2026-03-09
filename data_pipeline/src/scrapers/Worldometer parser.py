#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
import re
from typing import List, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# ----------------------------------------------------------------------
# Константы
# ----------------------------------------------------------------------
BASE_URL = "https://www.worldometers.info"
POPULATION_PAGE = urljoin(BASE_URL, "/population/")
CO2_PAGE = urljoin(BASE_URL, "/co2-emissions/")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; WorldometerScraper/1.0; +https://example.com/bot)"
    )
}
DELAY_BETWEEN_REQUESTS = 0.5  # seconds


# ----------------------------------------------------------------------
# Утилиты
# ----------------------------------------------------------------------
def fetch_page(url: str) -> str:
    """Вернуть «сырой» HTML‑текст страницы."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.text


def clean_num(text: Any) -> Any:
    """Удалить «мусор» и вернуть число (int/float) или None."""
    if pd.isna(text):
        return None

    s = str(text).strip()
    s = s.replace("\xa0", " ")               # неразрывный пробел
    s = re.sub(r"\[.*?\]", "", s)            # ссылки‑сноски [1] …
    s = re.sub(r"\(.*?\)", "", s)            # скобки с годами и т.п.
    s = s.replace(",", "")                   # разделители тысяч
    s = s.replace("%", "")                   # процентный знак
    s = s.replace("+", "")                   # плюс
    # оставляем минус, потому что он может означать отрицательное число
    s = s.replace(" ", "")                   # обычные пробелы

    if not s:
        return None

    try:
        return float(s) if "." in s else int(s)
    except ValueError:
        return None


def to_int(val: Any) -> Any:
    """Преобразовать float без дробной части в int, иначе оставить как есть."""
    if isinstance(val, float) and val.is_integer():
        return int(val)
    return val


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Общая очистка DataFrame (строковые пробелы + попытка числовой конвертации)."""
    # 1️⃣ Строковые столбцы → чистый текст
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.strip().replace(r"\s+", " ", regex=True)

    # 2️⃣ Пробуем превратить каждый столбец в числа
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        cleaned = df[col].apply(clean_num)
        if cleaned.notna().mean() > 0.5:          # >50 % → считаем числовым
            df[col] = cleaned

    return df


# ----------------------------------------------------------------------
# Парсинг списков стран
# ----------------------------------------------------------------------
def parse_population_list(html: str) -> List[Dict[str, str]]:
    """Получить список стран и их URL‑страниц с данными о населении."""
    soup = BeautifulSoup(html, "html.parser")
    ul = soup.select_one("ul:nth-child(19)")
    if not ul:
        raise RuntimeError("Не удалось найти список стран на странице населения.")

    countries = []
    for li in ul.find_all("li"):
        a = li.find("a")
        if a and a.get("href"):
            countries.append(
                {
                    "name": a.get_text(strip=True),
                    "url": urljoin(BASE_URL, a["href"]),
                }
            )
    print("✅ Список стран по населению получен.")
    return countries


def parse_co2_list(html: str) -> List[Dict[str, str]]:
    """Получить список стран и их URL‑страниц с данными о выбросах CO₂."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        raise RuntimeError("Не удалось найти таблицу со странами на странице CO₂.")

    links = []
    for a in table.select("td:nth-child(2) > a"):
        href = a.get("href")
        if href:
            links.append(
                {
                    "country": a.get_text(strip=True),
                    "url": urljoin(BASE_URL, href),
                }
            )
    print("✅ Список стран по эмиссии CO₂ получен.")
    return links


# ----------------------------------------------------------------------
# Парсинг таблиц конкретной страны
# ----------------------------------------------------------------------
def parse_population_tables(html: str, country: str) -> pd.DataFrame:
    """Собрать и очистить таблицы населения конкретной страны."""
    tables = pd.read_html(html, header=0)

    if len(tables) < 2:
        raise RuntimeError("На странице населения найдено менее двух таблиц.")

    df1, df2 = tables[0], tables[1]

    # Убираем суффиксы в заголовках
    df1.columns = df1.columns.str.replace(r"_historical$", "", regex=True)
    df2.columns = df2.columns.str.replace(r"_forecast$", "", regex=True)

    merged = pd.concat([df1, df2], ignore_index=True, sort=False)

    # Переименовываем последний столбец в «Global Rank», если он существует
    if merged.columns.size > 0:
        merged.rename(columns={merged.columns[-1]: "Global Rank"}, inplace=True)

    merged.insert(0, "Country", country)

    if "Year" in merged.columns:
        merged = merged.sort_values(by=["Country", "Year"], ascending=[True, False])

    # ---------- ОЧИСТКА ----------
    merged = clean_dataframe(merged)

    # ----- Специфическая очистка нужных столбцов -----
    # Global Rank → int
    if "Global Rank" in merged.columns:
        merged["Global Rank"] = merged["Global Rank"].apply(clean_num).apply(to_int)

    # Migrants (Net) → int (может быть отрицательным)
    if "Migrants (Net)" in merged.columns:
        merged["Migrants (Net)"] = merged["Migrants (Net)"].apply(clean_num).apply(to_int)

    # ----------------------------------------------

    print(f"✅ Таблицы населения {country} обработаны.")
    return merged


def parse_co2_tables(html: str, country: str) -> pd.DataFrame:
    """Собрать и очистить таблицы выбросов CO₂ конкретной страны."""
    tables = pd.read_html(html, header=0)

    if not tables:
        raise RuntimeError("На странице CO₂ не найдено таблиц.")

    if len(tables) >= 2:
        df1, df2 = tables[0], tables[1]
        df1.columns = df1.columns.str.replace(r"_historical$", "", regex=True)
        df2.columns = df2.columns.str.replace(r"_forecast$", "", regex=True)
        df = pd.concat([df1, df2], ignore_index=True, sort=False)
    else:
        df = tables[0]

    df.columns = df.columns.str.replace(r"_historical$", "", regex=True)
    df.columns = df.columns.str.replace(r"_forecast$", "", regex=True)

    for col in ["Population", "Pop. change"]:
        if col in df.columns:
            df.drop(columns=col, inplace=True)

    df.insert(0, "Country", country)

    if "Year" in df.columns:
        df = df.sort_values(by=["Country", "Year"], ascending=[True, False])

    # ---------- ОЧИСТКА ----------
    df = clean_dataframe(df)

    # ----- Специфическая очистка нужных столбцов -----
    # Global Rank → int
    if "Global Rank" in df.columns:
        df["Global Rank"] = df["Global Rank"].apply(clean_num).apply(to_int)

    # Migrants (Net) → int (если присутствует в CO₂‑таблице)
    if "Migrants (Net)" in df.columns:
        df["Migrants (Net)"] = df["Migrants (Net)"].apply(clean_num).apply(to_int)

    # ----------------------------------------------

    print(f"✅ Таблицы CO₂ {country} обработаны.")
    return df


# ----------------------------------------------------------------------
# Основная функция
# ----------------------------------------------------------------------
def main():
    # ---------------------- CO₂ ----------------------
    print("\n📥 Скачиваем страницу CO₂‑эмиссий…")
    co2_html = fetch_page(CO2_PAGE)

    print("🔎 Парсим список стран…")
    co2_countries = parse_co2_list(co2_html)
    print(f"✅ Найдено {len(co2_countries)} стран для CO₂.\n")

    co2_data: List[pd.DataFrame] = []
    for idx, entry in enumerate(co2_countries, start=1):
        name, url = entry["country"], entry["url"]
        print(f"[{idx}/{len(co2_countries)}] Обрабатываем {name}…")
        try:
            country_html = fetch_page(url)
            df = parse_co2_tables(country_html, name)
            df["CO2 URL"] = url
            co2_data.append(df)
        except Exception as exc:
            print(f"⚠️ Ошибка при обработке {name}: {exc}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if co2_data:
        co2_df = pd.concat(co2_data, ignore_index=True)
        co2_file = "worldometer_co2_data.csv"
        co2_df.to_csv(co2_file, index=False, encoding="utf-8-sig")
        print(f"\n✅ Данные CO₂ сохранены в «{co2_file}»")
    else:
        print("\n❌ Не удалось собрать ни одной строки CO₂‑данных.")

    # ---------------------- POPULATION ----------------------
    print("\n📥 Скачиваем страницу населения…")
    pop_html = fetch_page(POPULATION_PAGE)

    print("🔎 Парсим список стран…")
    pop_countries = parse_population_list(pop_html)
    print(f"✅ Найдено {len(pop_countries)} стран для населения.\n")

    pop_data: List[pd.DataFrame] = []
    for idx, entry in enumerate(pop_countries, start=1):
        name, url = entry["name"], entry["url"]
        print(f"[{idx}/{len(pop_countries)}] Обрабатываем {name}…")
        try:
            country_html = fetch_page(url)
            df = parse_population_tables(country_html, name)
            df["Population URL"] = url
            pop_data.append(df)
        except Exception as exc:
            print(f"⚠️ Ошибка при обработке {name}: {exc}")
        time.sleep(DELAY_BETWEEN_REQUESTS)

    if pop_data:
        pop_df = pd.concat(pop_data, ignore_index=True)
        #pop_df["Global Rank"] = pop_df["Global Rank"].astype("int64")
        #pop_df["Yearly % Change"] = pop_df["Yearly % Change"].astype("float64")
        #pop_df["Yearly Change"] = pop_df["Yearly Change"].astype("int64")
        pop_file = "worldometer_population_data.csv"
        pop_df.to_csv(pop_file, index=False, encoding="utf-8-sig")
        print(f"\n✅ Данные о населении сохранены в «{pop_file}»")
    else:
        print("\n❌ Не удалось собрать ни одной строки данных о населении.")

    # ---------------------- ОТЧЁТ ----------------------
    print("\n🔎 Первые 5 строк объединённого DataFrame (CO₂):")
    if co2_data:
        print(co2_df.head())
        print(co2_df.info())
    print("\n🔎 Первые 5 строк объединённого DataFrame (Population):")
    if pop_data:
        print(pop_df.head())
        print(pop_df.info())


if __name__ == "__main__":
    main() 
    df = pd.read_csv("worldometer_population_data.csv")
    print(df["Migrants (net)"])
