import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
import hashlib
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

SAPO_URL = "https://sapo.pt/tags/inteligencia-artificial"
VB_API = "https://venturebeat.com/wp-json/wp/v2"

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


def get_json(url, params=None):
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        return r.json()
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


# -------- VENTUREBEAT (WordPress REST API) --------
def scrape_venturebeat():
    # Resolve the numeric ID of the "ai" category
    categories = get_json(f"{VB_API}/categories", params={"slug": "ai"})
    if not categories:
        logging.warning("VentureBeat: falhou ao obter categoria 'ai' via API")
        return []

    ai_id = categories[0]["id"]
    logging.info(f"VentureBeat categoria 'ai' id={ai_id}")

    articles = []
    page = 1

    while True:
        posts = get_json(
            f"{VB_API}/posts",
            params={
                "categories": ai_id,
                "per_page": 100,
                "page": page,
                "_fields": "title,link",
            },
        )

        if not posts:
            break

        for post in posts:
            title = clean(BeautifulSoup(post["title"]["rendered"], "html.parser").get_text())
            url = post["link"]
            if title and url:
                articles.append({"title": title, "url": url, "source": "venturebeat"})

        logging.info(f"VentureBeat página {page}: {len(posts)} artigos")

        if len(posts) < 100:
            break

        page += 1
        time.sleep(0.3)

    logging.info(f"VentureBeat: {len(articles)} artigos no total")
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
    vb = scrape_venturebeat()

    all_articles = sapo + vb

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
