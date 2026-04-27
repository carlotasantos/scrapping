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

    # Anti-detecção (sempre activo)
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

    # Remover flag navigator.webdriver para evitar detecção
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })

    driver.get(VENTUREBEAT_URL)
    time.sleep(4)

    # Remover overlays/modais
    driver.execute_script("""
        document.querySelectorAll('[class*="modal"], [class*="dialog"], [class*="popup"], [id*="modal"]').forEach(e => e.style.display = 'none');
        document.querySelectorAll('*').forEach(el => {
            const style = window.getComputedStyle(el);
            if (style.position === 'fixed' && el.offsetHeight > 150) el.remove();
        });
    """)

    time.sleep(2)

    # Scroll até ao fundo + scrollIntoView no último artigo para disparar IntersectionObserver
    last_article_count = 0
    no_change_count = 0
    scrolls_done = 0
    max_scrolls = 60

    while scrolls_done < max_scrolls:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(2)

        try:
            page_articles = driver.find_elements(By.TAG_NAME, "article")
            if page_articles:
                driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'end', behavior: 'smooth'})",
                    page_articles[-1]
                )
                time.sleep(1.5)
        except:
            pass

        current_article_count = len(driver.find_elements(By.TAG_NAME, "article"))

        if current_article_count > last_article_count:
            no_change_count = 0
            last_article_count = current_article_count
            logging.info(f"Artigos carregados: {current_article_count}")
        else:
            no_change_count += 1
            if no_change_count >= 5:
                break

        scrolls_done += 1

    logging.info(f"Scroll finalizado. Total de scrolls: {scrolls_done}, Artigos encontrados: {last_article_count}")

    time.sleep(2)

    # Debug: salvar HTML para inspecção
    html_debug = driver.page_source
    with open(os.path.join(DATA_DIR, "venturebeat_debug.html"), "w", encoding="utf-8") as f:
        f.write(html_debug)

    soup = BeautifulSoup(html_debug, "html.parser")
    driver.quit()

    articles = []
    seen = set()

    SKIP_SEGMENTS = {"/category/", "/tag/", "/author/", "/events/", "/newsletter/", "/author-page/"}

    def is_article_url(url):
        if "venturebeat.com" not in url:
            return False
        if any(x in url for x in SKIP_SEGMENTS) or any(x in url for x in ["mailto:", "javascript:", "?", "#"]):
            return False
        path = url.replace("https://venturebeat.com", "").strip("/")
        # article URLs have at least one slug segment with hyphens
        return path.count("/") >= 1 and "-" in path

    # Procurar artigos directamente nos elementos <article>
    article_elements = soup.find_all("article")
    logging.info(f"VentureBeat elementos <article> encontrados: {len(article_elements)}")

    for article in article_elements:
        # Título: preferir heading semântico dentro do artigo
        title_el = article.find(["h2", "h3", "h4"]) or article.find("header")
        title = clean(title_el.get_text()) if title_el else None

        # URL: primeiro link com href de artigo válido
        url = None
        for a in article.find_all("a", href=True):
            candidate = urljoin(VENTUREBEAT_URL, a.get("href", ""))
            if is_article_url(candidate):
                url = candidate
                break

        if not url or not title or len(title) < 3:
            continue

        if url in seen:
            continue

        seen.add(url)
        articles.append({"title": title, "url": url, "source": "venturebeat"})
        logging.info(f"✓ Artigo: {title[:60]} | {url}")

    # Fallback: procurar links em toda a página se poucos artigos foram encontrados
    if len(articles) < 20:
        logging.info("Poucos artigos encontrados nos elementos <article>, tentando fallback...")

        all_links = soup.find_all("a", href=True)
        for a in all_links:
            url = urljoin(VENTUREBEAT_URL, a.get("href", ""))
            title = clean(a.get_text())

            if not is_article_url(url):
                continue

            if not title or len(title) < 3:
                continue

            if url in seen:
                continue

            seen.add(url)
            articles.append({"title": title, "url": url, "source": "venturebeat"})
            logging.info(f"✓ Artigo (fallback): {title[:60]} | {url}")

    logging.info(f"VentureBeat: {len(articles)} artigos únicos")
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
