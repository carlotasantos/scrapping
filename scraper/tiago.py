import json
import time
import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException


# Config
OUTPUT_FILE = "ai_news.json"
LOG_FILE = "logs_ai_news.log"

MIN_TITLE = 15

SOURCES = [
    {
        "name": "bbc_tech",
        "urls": ["https://www.bbc.com/innovation/technology"],
        "scroll": 6,
        "base": "https://www.bbc.com"
    },
    {
        "name": "ars_technica",
        "urls": [
            "https://arstechnica.com/ai/",
            "https://arstechnica.com/ai/page/2/",
            "https://arstechnica.com/ai/page/3/"
        ],
        "scroll": 3,
        "base": "https://arstechnica.com"
    },
    {
        "name": "techcrunch_ai",
        "urls": [
            "https://techcrunch.com/category/artificial-intelligence/",
            "https://techcrunch.com/category/artificial-intelligence/page/2/"
        ],
        "scroll": 3,
        "base": "https://techcrunch.com"
    }
]


# Logging
open(LOG_FILE, "a").close()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

log = logging.getLogger("ai")


# Drives
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20)  # 20 segundos de timeout para carregamento de página para evitar travamentos
    return driver


# Auxiliares
def make_id(url):
    return hashlib.md5(url.encode()).hexdigest()

def clean(text):
    return re.sub(r"\s+", " ", text or "").strip()

def scroll(driver, n):
    last = driver.execute_script("return document.body.scrollHeight")

    for i in range(n):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

        new = driver.execute_script("return document.body.scrollHeight")
        if new == last:
            break
        last = new


# JSON
def get_json_ld(soup):
    scripts = soup.find_all("script", type="application/ld+json")

    for s in scripts:
        try:
            data = json.loads(s.get_text())

            if isinstance(data, list):
                data = data[0]

            if isinstance(data, dict):
                return data
        except:
            continue

    return {}


# Autor
def get_author(soup):
    selectors = [
        ".author",
        ".byline",
        "[rel='author']",
        "a[rel='author']",
        "span[class*='author']"
    ]

    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            return clean(el.get_text())

    data = get_json_ld(soup)
    author = data.get("author")

    if isinstance(author, dict):
        return author.get("name")

    if isinstance(author, list):
        if len(author) > 0 and isinstance(author[0], dict):
            return author[0].get("name")
        return author[0] if author else None

    if isinstance(author, str):
        return author

    return None


# Data
def get_date(soup):
    t = soup.find("time")
    if t:
        return t.get("datetime") or clean(t.get_text())

    meta = soup.select_one("meta[property='article:published_time']")
    if meta:
        return meta.get("content")

    meta2 = soup.select_one("meta[name='pubdate']")
    if meta2:
        return meta2.get("content")

    data = get_json_ld(soup)
    return data.get("datePublished") or data.get("dateCreated")


# Conteudo
def fetch_content(driver):
    soup = BeautifulSoup(driver.page_source, "lxml")

    paragraphs = soup.select("article p, main p")

    text = []
    for p in paragraphs:
        t = clean(p.get_text())
        if len(t) > 40:
            text.append(t)

    return "\n".join(text[:10])


# Link 
def extract_link(a, base):
    href = a.get("href")
    title = clean(a.get_text())

    if not href or len(title) < MIN_TITLE:
        return None

    if href.startswith("/"):
        href = base + href

    if not href.startswith("http"):
        return None

    return {
        "title": title,
        "url": href
    }


# Parsers
def parse_generic(soup, selectors, base, source):
    links = []

    for sel in selectors:
        links.extend(soup.select(sel))

    seen = set()
    items = []

    for a in links:
        item = extract_link(a, base)
        if not item:
            continue

        if item["url"] in seen:
            continue

        seen.add(item["url"])
        item["source"] = source
        items.append(item)

    return items

def parse_bbc(soup):
    return parse_generic(
        soup,
        ["article a[href]", "a[href*='/innovation/']"],
        "https://www.bbc.com",
        "bbc_tech"
    )

def parse_ars(soup):
    return parse_generic(
        soup,
        ["article h2 a", "article h3 a"],
        "https://arstechnica.com",
        "ars_technica"
    )

def parse_tc(soup):
    return parse_generic(
        soup,
        ["article h2 a", ".post-block__title a"],
        "https://techcrunch.com",
        "techcrunch_ai"
    )

PARSERS = {
    "bbc_tech": parse_bbc,
    "ars_technica": parse_ars,
    "techcrunch_ai": parse_tc
}


# Guardar e carregar dados
def load():
    if not os.path.exists(OUTPUT_FILE):
        return []
    try:
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return []

def save(data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


# MAIN
def run():
    log.info("FULL DETAIL scraper started")

    driver = create_driver()

    existing = load()
    existing_ids = {x["id"] for x in existing}

    new_items = []
    seen_total = 0

    try:
        for src in SOURCES:
            parser = PARSERS[src["name"]]

            for url in src["urls"]:
                log.info(f"{src['name']} -> {url}")

                driver.get(url)
                time.sleep(3)

                scroll(driver, src["scroll"])

                soup = BeautifulSoup(driver.page_source, "lxml")
                items = parser(soup)

                seen_total += len(items)

                for raw in items:
                    item_id = make_id(raw["url"])

                    if item_id in existing_ids:
                        continue

                    try:
                        driver.get(raw["url"])
                        time.sleep(2)
                    except TimeoutException:
                        continue

                    soup_article = BeautifulSoup(driver.page_source, "lxml")

                    content = fetch_content(driver)
                    author = get_author(soup_article)
                    date = get_date(soup_article)

                    new_items.append({
                        "id": item_id,
                        "title": raw["title"],
                        "content": content,
                        "source": {
                            "name": raw["source"],
                            "domain": urlparse(raw["url"]).netloc
                        },
                        "url": raw["url"],
                        "published_at": date,
                        "collected_at": datetime.now(timezone.utc).isoformat(),
                        "author": author
                    })

                time.sleep(1)

    finally:
        driver.quit()

    save(existing + new_items)

    log.info(f"Seen: {seen_total}")
    log.info(f"New: {len(new_items)}")

if __name__ == "__main__":
    run()
