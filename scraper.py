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
def scrape_sapo(known_urls):
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

        new_on_page = 0
        for a in items:
            link = a.select_one("h3 a")
            if not link:
                continue
            url = urljoin(SAPO_URL, link["href"])
            if url in known_urls:
                continue
            new_on_page += 1
            articles.append({
                "title": clean(link.get_text()),
                "url": url,
                "source": "sapo"
            })

        if new_on_page == 0:
            logging.info(f"SAPO: sem artigos novos na página {page}, a parar")
            break

        page += 1

    logging.info(f"SAPO: {len(articles)} artigos novos")
    return articles


# -------- TOWARDS DATA SCIENCE (Selenium) --------
def scrape_tds(known_urls):
    options = Options()

    if os.getenv("CI"):
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(120)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })

    articles = []
    seen = set()
    page = 1
    max_pages = 250

    while page <= max_pages:
        page_url = TDS_URL if page == 1 else f"https://towardsdatascience.com/latest/page/{page}/"
        try:
            driver.get(page_url)
        except Exception as e:
            logging.error(f"TDS: erro ao carregar {page_url}: {e}")
            break
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        items = soup.select("ul.wp-block-post-template li")

        if not items:
            logging.info(f"TDS: sem artigos na página {page}, a parar")
            break

        new_on_page = 0
        for li in items:
            title_el = li.find(["h2", "h3", "h4"])
            title = clean(title_el.get_text()) if title_el else None

            a = li.find("a", href=True)
            url = urljoin(TDS_URL, a["href"]) if a else None

            if not title or not url or len(title) < 3:
                continue
            if url in seen or url in known_urls:
                continue

            new_on_page += 1
            seen.add(url)
            articles.append({"title": title, "url": url, "source": "towardsdatascience"})
            logging.info(f"✓ TDS p{page}: {title[:60]} | {url}")

        logging.info(f"TDS página {page}: {new_on_page} artigos novos")

        if new_on_page == 0:
            logging.info("TDS: sem artigos novos, a parar")
            break

        page += 1

    driver.quit()
    logging.info(f"TDS: {len(articles)} artigos novos no total")
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

    # Load existing articles and build known-URL index
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = []

    known_urls = {art["url"] for art in existing}
    logging.info(f"Artigos existentes: {len(existing)}")

    sapo = scrape_sapo(known_urls)
    tds = scrape_tds(known_urls)

    new_articles = sapo + tds
    new_results = []
    seen = set(known_urls)

    for art in new_articles:
        if art["url"] in seen:
            continue

        seen.add(art["url"])

        desc = extract_content(art["url"]) if art["source"] == "sapo" else None

        new_results.append({
            "id": make_id(art["url"]),
            "title": art["title"],
            "url": art["url"],
            "source": art["source"],
            "description": desc,
            "published_at": None,
            "scraped_at": now
        })

        time.sleep(0.5)

    if not new_results:
        logging.info("Sem artigos novos, ficheiro mantido sem alterações")
        return

    results = new_results + existing

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(f"Novos: {len(new_results)} | Total: {len(results)}")


if __name__ == "__main__":
    scrape()
