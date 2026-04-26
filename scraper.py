import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timezone
import logging
import os
import re
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "imdb.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

URL = "https://www.imdb.com/chart/top/"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}


def clean_text(value):
    return " ".join(value.split()) if value else None


def clean_title(value):
    value = clean_text(value)
    if not value:
        return None
    return re.sub(r"^#?\d+[\.\s]+", "", value).strip()


def parse_rating(value):
    value = clean_text(value)
    if not value:
        return None

    match = re.search(r"\d+(?:[.,]\d+)?", value)
    if not match:
        return None

    return float(match.group(0).replace(",", "."))


def parse_votes(value):
    value = clean_text(value)
    if not value:
        return None

    match = re.search(r"\(([^)]+)\)", value)
    if match:
        return match.group(1).strip()

    return None


def fetch_with_requests():
    response = requests.get(URL, headers=HEADERS, timeout=30)
    logging.info("Requests response: status=%s bytes=%s", response.status_code, len(response.text))
    response.raise_for_status()

    if "awswaf" in response.text.lower() or "gokuprops" in response.text.lower():
        logging.warning("IMDb returned AWS WAF page to requests")
        return None

    return response.text


def fetch_with_selenium():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait

    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"user-agent={HEADERS['User-Agent']}")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get(URL)
        WebDriverWait(driver, 30).until(
            lambda browser: browser.find_elements(
                By.CSS_SELECTOR,
                "li.ipc-metadata-list-summary-item, .ipc-metadata-list-summary-item",
            )
        )

        previous_count = 0
        stable_rounds = 0
        for _ in range(12):
            item_count = len(driver.find_elements(By.CSS_SELECTOR, "li.ipc-metadata-list-summary-item"))
            if item_count >= 250:
                break

            if item_count == previous_count:
                stable_rounds += 1
            else:
                stable_rounds = 0

            if stable_rounds >= 3:
                break

            previous_count = item_count
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1)

        return driver.page_source
    finally:
        driver.quit()


def parse_movie_item(item, scraped_at):
    title_el = item.select_one("h3.ipc-title__text, h3")
    rating_value_el = item.select_one(".ipc-rating-star--rating")
    rating_container_el = item.select_one(".ipc-rating-star")
    metadata = [
        clean_text(el.get_text(" ", strip=True))
        for el in item.select(".cli-title-metadata li, .cli-title-metadata span")
    ]
    metadata = [value for value in metadata if value]

    title = clean_title(title_el.get_text(" ", strip=True) if title_el else None)
    if not title:
        return None

    rating_text = rating_value_el.get_text(" ", strip=True) if rating_value_el else None
    rating_container_text = rating_container_el.get_text(" ", strip=True) if rating_container_el else None
    return {
        "title": title,
        "source": "imdb",
        "rating": parse_rating(rating_text or rating_container_text),
        "votes": parse_votes(rating_container_text),
        "year": metadata[0] if len(metadata) > 0 else None,
        "duration": metadata[1] if len(metadata) > 1 else None,
        "age_rating": metadata[2] if len(metadata) > 2 else None,
        "genre": None,
        "timestamp": scraped_at
    }


def parse_movies(html):
    soup = BeautifulSoup(html, "html.parser")
    movies = []
    scraped_at = datetime.now(timezone.utc).isoformat()

    for item in soup.select("li.ipc-metadata-list-summary-item, .ipc-metadata-list-summary-item"):
        try:
            movie = parse_movie_item(item, scraped_at)
            if movie:
                movies.append(movie)
        except Exception:
            logging.exception("Erro parsing item")

    return movies

def scrape():
    logging.info("Start scraping")

    html = fetch_with_requests()
    if not html:
        logging.info("Falling back to Selenium")
        html = fetch_with_selenium()

    movies = parse_movies(html)

    save_json(movies)
    logging.info(f"{len(movies)} filmes guardados")

def save_json(data):
    path = os.path.join(DATA_DIR, "imdb.json")

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    scrape()
