import requests
from bs4 import BeautifulSoup
import json
import time
import logging
import os
import hashlib
import re
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


def parse_date(text):
    text = clean(text)
    if not text:
        return None

    value = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(value).isoformat()
    except ValueError:
        pass

    match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if match:
        return match.group(0)

    months = {
        "janeiro": "01",
        "january": "01",
        "fevereiro": "02",
        "february": "02",
        "marco": "03",
        "março": "03",
        "march": "03",
        "abril": "04",
        "april": "04",
        "maio": "05",
        "may": "05",
        "junho": "06",
        "june": "06",
        "julho": "07",
        "july": "07",
        "agosto": "08",
        "august": "08",
        "setembro": "09",
        "september": "09",
        "outubro": "10",
        "october": "10",
        "novembro": "11",
        "november": "11",
        "dezembro": "12",
        "december": "12",
    }
    pattern = r"(\d{1,2})\s+(?:de\s+)?([a-zç]+)\s+(?:de\s+)?(\d{4})(?:\s+(\d{1,2}):(\d{2}))?"
    match = re.search(pattern, text.lower())
    if not match:
        pattern = r"([a-z]+)\s+(\d{1,2}),\s+(\d{4})(?:\s+(\d{1,2}):(\d{2}))?"
        match = re.search(pattern, text.lower())
        if match:
            month_name, day, year, hour, minute = match.groups()
        else:
            return None
    else:
        day, month_name, year, hour, minute = match.groups()

    month = months.get(month_name)
    if not month:
        return None

    date = f"{year}-{month}-{int(day):02d}"
    if hour and minute:
        return f"{date}T{int(hour):02d}:{minute}:00"
    return date


def extract_published_at_from_soup(soup):
    selectors = [
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[itemprop="datePublished"]',
        'meta[name="date"]',
        'meta[name="pubdate"]',
        'time[datetime]'
    ]

    for selector in selectors:
        element = soup.select_one(selector)
        if not element:
            continue

        value = element.get("content") or element.get("datetime") or element.get_text()
        parsed = parse_date(value)
        if parsed:
            return parsed

    for element in soup.select("time, .metadata, .meta"):
        parsed = parse_date(element.get_text())
        if parsed:
            return parsed

    return None


BOILERPLATE_PHRASES = [
    "esta voz foi gerada com recurso a inteligência artificial",
    "este resumo foi criado com recurso a inteligência artificial",
    "a tua opinião é importante para ajudar a melhorar esta funcionalidade",
    "se consideras que o áudio não está claro",
]


def is_useful_text(text):
    text = clean(text)
    if not text:
        return False

    lower = text.lower()
    if "publicidade" in lower:
        return False

    return not any(phrase in lower for phrase in BOILERPLATE_PHRASES)



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
            excerpt_el = li.select_one(".wp-block-post-excerpt__excerpt")
            description = clean(excerpt_el.get_text()) if excerpt_el else None
            date_el = li.select_one(".wp-block-post-date time[datetime], time[datetime], .wp-block-post-date time, time, .wp-block-post-date")
            published_at = None
            if date_el:
                published_at = parse_date(date_el.get("datetime") or date_el.get_text())

            if not title or not url or len(title) < 3:
                continue
            if url in seen or url in known_urls:
                continue

            new_on_page += 1
            seen.add(url)
            articles.append({
                "title": title,
                "url": url,
                "source": "towardsdatascience",
                "description": description,
                "published_at": published_at
            })
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
def extract_article_data(url):
    html = get_html(url)
    if not html:
        return None, None

    soup = BeautifulSoup(html, "html.parser")
    published_at = extract_published_at_from_soup(soup)

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    selectors = [
        "article p",
        "main article p",
        "main p",
        ".article-content p",
        ".entry-content p",
        ".post-content p",
        ".content p"
    ]

    paragraphs = []
    for selector in selectors:
        paragraphs = [
            clean(p.get_text())
            for p in soup.select(selector)
            if clean(p.get_text())
        ]
        paragraphs = [
            p for p in paragraphs
            if len(p) >= 80 and is_useful_text(p)
        ]
        if paragraphs:
            break

    if paragraphs:
        return clean(" ".join(paragraphs[:3])), published_at

    meta = soup.select_one('meta[name="description"], meta[property="og:description"]')
    if meta and meta.get("content") and is_useful_text(meta["content"]):
        return clean(meta["content"]), published_at

    p = soup.select_one("p")
    text = clean(p.get_text()) if p else None
    return text if is_useful_text(text) else None, published_at


def extract_content(url):
    content, _ = extract_article_data(url)
    return content


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

        desc = art.get("description")
        published_at = art.get("published_at")
        if not desc and art["source"] == "sapo":
            desc, published_at = extract_article_data(art["url"])

        new_results.append({
            "id": make_id(art["url"]),
            "title": art["title"],
            "url": art["url"],
            "source": art["source"],
            "description": desc,
            "published_at": published_at,
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
