import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
import hashlib
from datetime import datetime, UTC
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# -------- PATHS --------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

OUTPUT_FILE = os.path.join(DATA_DIR, "scraper.json")

# -------- LOGGING --------
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "scraper.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

SAPO_URL = "https://sapo.pt/tags/inteligencia-artificial"
TDS_URL = "https://towardsdatascience.com/latest/"

HEADERS = {"User-Agent": "Mozilla/5.0"}


def clean(text):
    return " ".join(text.split()) if text else None


def make_id(url):
    return hashlib.md5(url.encode()).hexdigest()


def get_html(url, params=None):
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.text
    except:
        return None



# -------- SAPO --------
def scrape_sapo():
    articles = []
    page = 1

    while True:
        html = get_html(SAPO_URL, params={"pagina": page})
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("article.article-default")

        if not items:
            break

        for a in items:
            link = a.select_one("h3 a")
            if not link:
                continue

            articles.append({
                "title": clean(link.get_text()),
                "url": urljoin(SAPO_URL, link["href"]),
                "source": "sapo"
            })

        page += 1

    logging.info(f"SAPO: {len(articles)}")
    return articles


# -------- TOWARDS DATA SCIENCE (Selenium) --------
def scrape_tds():
    options = Options()

    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if os.getenv("CI"):
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })

    driver.get(TDS_URL)
    time.sleep(4)

    last_count = 0
    no_change = 0
    scrolls = 0
    max_scrolls = 60

    while scrolls < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        try:
            items = driver.find_elements(By.CSS_SELECTOR, "ul.wp-block-post-template li")
            if items:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'end', behavior: 'smooth'})",
                    items[-1]
                )
                time.sleep(1.5)
        except:
            pass

        current_count = len(driver.find_elements(By.CSS_SELECTOR, "ul.wp-block-post-template li"))

        if current_count > last_count:
            no_change = 0
            last_count = current_count
            logging.info(f"TDS artigos carregados: {current_count}")
        else:
            no_change += 1
            if no_change >= 5:
                break

        scrolls += 1

    logging.info(f"TDS scroll finalizado: {scrolls} scrolls, {last_count} artigos")

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    articles = []
    seen = set()

    for li in soup.select("ul.wp-block-post-template li"):
        title_el = li.find(["h2", "h3", "h4"])
        title = clean(title_el.get_text()) if title_el else None

        a = li.find("a", href=True)
        url = urljoin(TDS_URL, a["href"]) if a else None

        if not title or not url or len(title) < 3:
            continue
        if url in seen:
            continue

        seen.add(url)
        articles.append({"title": title, "url": url, "source": "towardsdatascience"})
        logging.info(f"✓ TDS: {title[:60]} | {url}")

    logging.info(f"TDS: {len(articles)} artigos únicos")
    return articles


# -------- CONTENT --------
def extract_content(url):
    html = get_html(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    p = soup.select_one("p")
    return clean(p.get_text()) if p else None


# -------- MAIN --------
def scrape():
    now = datetime.now(UTC).isoformat()

    sapo = scrape_sapo()
    tds = scrape_tds()

    all_articles = sapo + tds

    results = []
    seen = set()

    for art in all_articles:
        if art["url"] in seen:
            continue

        seen.add(art["url"])

        desc = extract_content(art["url"]) if art["source"] == "sapo" else None

        results.append({
            "id": make_id(art["url"]),
            "title": art["title"],
            "url": art["url"],
            "source": art["source"],
            "description": desc,
            "published_at": None,
            "scraped_at": now
        })

        time.sleep(0.5)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(f"Total: {len(results)}")


if __name__ == "__main__":
    scrape()
