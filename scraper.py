import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
from datetime import datetime, UTC
from urllib.parse import urljoin

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

# -------- URLS --------
SAPO_URL = "https://sapo.pt/tags/inteligencia-artificial"
VENTUREBEAT_URL = "https://venturebeat.com/category/ai"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def clean(text):
    return " ".join(text.split()) if text else None


def get_html(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.text


# -------- SAPO (ESTÁTICO) --------
def scrape_sapo(pages=3):
    articles = []

    for page in range(1, pages + 1):
        logging.info(f"SAPO page {page}")
        html = get_html(SAPO_URL, params={"pagina": page})
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.select("article.article-default"):
            link = a.select_one("h3 a")
            if not link:
                continue

            url = urljoin(SAPO_URL, link["href"])
            title = clean(link.get_text())

            articles.append({
                "title": title,
                "url": url,
                "source": "sapo"
            })

    return articles


# -------- VENTUREBEAT (DINÂMICO) --------
def scrape_venturebeat():
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.get(VENTUREBEAT_URL)

    time.sleep(2)

    # remover popups (com retry)
    for _ in range(6):
        driver.execute_script("""
            document.querySelectorAll(
                '.modal, .popup, .overlay, .newsletter, .cookie, .consent, .onetrust-banner-sdk'
            ).forEach(el => el.remove());

            document.querySelectorAll('button').forEach(btn => {
                const t = (btn.innerText || '').toLowerCase();
                if (t.includes('accept') || t.includes('agree') || t.includes('close')) {
                    try { btn.click(); } catch(e){}
                }
            });
        """)
        time.sleep(1)

    # scroll forte (lazy loading)
    for _ in range(15):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    articles = []
    seen = set()

    for article in soup.select("article.flex.flex-col"):
        link = article.select_one("h2 a, h3 a")
        if not link:
            continue

        url = urljoin(VENTUREBEAT_URL, link.get("href"))
        title = clean(link.get_text())

        if not url or not title:
            continue

        if any(x in url for x in ["/category/", "/tag/", "/author/"]):
            continue

        if url in seen:
            continue

        seen.add(url)

        articles.append({
            "title": title,
            "url": url,
            "source": "venturebeat"
        })

    logging.info(f"VentureBeat articles: {len(articles)}")

    return articles


# -------- CONTEÚDO --------
def extract_content(url):
    try:
        html = get_html(url)
        soup = BeautifulSoup(html, "html.parser")

        paragraphs = [
            clean(p.get_text())
            for p in soup.select("p")
            if len(p.get_text()) > 50
        ]

        return {
            "description": paragraphs[0] if paragraphs else None,
            "word_count": sum(len(p.split()) for p in paragraphs)
        }

    except Exception as e:
        logging.error(f"Erro content {url}: {e}")
        return {
            "description": None,
            "word_count": 0
        }


# -------- MAIN --------
def scrape():
    now = datetime.now(UTC).isoformat()

    logging.info("Start scraping")

    sapo = scrape_sapo()
    vb = scrape_venturebeat()

    all_articles = sapo + vb

    results = []
    seen = set()

    for art in all_articles:
        if art["url"] in seen:
            continue

        seen.add(art["url"])

        details = extract_content(art["url"])

        results.append({
            "id": hash(art["url"]),
            "title": art["title"],
            "url": art["url"],
            "source": art["source"],
            "description": details["description"],
            "published_at": None,
            "scraped_at": now
        })

        time.sleep(1)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    logging.info(f"Saved {len(results)} articles")

    return results


if __name__ == "__main__":
    scrape()