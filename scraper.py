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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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
VENTUREBEAT_URL = "https://venturebeat.com/category/ai"

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


# -------- VENTUREBEAT --------
def scrape_venturebeat():
    options = Options()

    # modo CI (GitHub Actions)
    if os.getenv("CI"):
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")

    # tamanho janela (mantém sempre)
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 10)

    driver.get(VENTUREBEAT_URL)
    time.sleep(3)

    # remover overlays
    driver.execute_script("""
        document.querySelectorAll('[class*="modal"], [class*="dialog"], [class*="popup"], [id*="modal"]').forEach(e => e.style.display = 'none');
        document.querySelectorAll('*').forEach(el => {
            const style = window.getComputedStyle(el);
            if (style.position === 'fixed' && el.offsetHeight > 150) {
                el.remove();
            }
        });
    """)

    time.sleep(2)

    # scroll agressivo com espera por elementos
    last_height = 0
    no_change_count = 0
    for scroll_num in range(100):
        driver.execute_script("window.scrollBy(0, 1500)")
        time.sleep(0.8)

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            no_change_count += 1
            if no_change_count >= 5:
                break
        else:
            no_change_count = 0
        last_height = new_height

    time.sleep(2)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    driver.quit()

    articles = []
    seen = set()

    # Procurar todos os links relevantes
    for a in soup.find_all("a", href=True):
        url = urljoin(VENTUREBEAT_URL, a["href"])
        title = clean(a.get_text())

        # filtros: URL deve conter venturebeat e ser um artigo
        if "venturebeat.com" not in url:
            continue

        # descartar categorias, tags, etc
        if any(x in url for x in ["/category/", "/tag/", "/author/", "/events/", "/newsletter/", "/author-page/", "?", "#"]):
            continue

        # deve ter título e URL válida
        if not title or len(title) < 3:
            continue

        # URL deve parecer um artigo
        if "/ai/" not in url and "/news/" not in url:
            continue

        if url in seen:
            continue

        seen.add(url)

        articles.append({
            "title": title,
            "url": url,
            "source": "venturebeat"
        })

    logging.info(f"VentureBeat: {len(articles)}")
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
