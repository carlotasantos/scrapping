import hashlib
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_DIR, "ai_news.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

BASE_URL = "https://sapo.pt"
TAG_URL = f"{BASE_URL}/tags/inteligencia-artificial"
VENTUREBEAT_BASE_URL = "https://venturebeat.com"
VENTUREBEAT_AI_URL = f"{VENTUREBEAT_BASE_URL}/category/ai"
OUTPUT_FILE = os.path.join(DATA_DIR, "ai_news.json")
MAX_PAGES = 10
MIN_PARAGRAPH_LENGTH = 40
DYNAMIC_FALLBACK_WORD_LIMIT = 80
VENTUREBEAT_SCROLL_STEPS = 12

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/146.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7",
}

SKIP_PARAGRAPH_PATTERNS = [
    "esta voz foi gerada",
    "este resumo foi criado",
    "envia o teu feedback",
    "a tua opinião é importante",
    "ler resumo",
    "ouvir resumo",
    "partilhar",
]


def clean_text(value):
    return " ".join(value.split()) if value else None


def absolute_url(value, base_url=BASE_URL):
    if not value:
        return None
    return urljoin(base_url, value)


def make_article_id(source, url, raw_id=None):
    if raw_id:
        return str(raw_id)

    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]
    return f"{source}_{digest}"


def parse_published_datetime(value):
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.isoformat()
    except ValueError:
        pass

    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4}).*?(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if match:
        day, month, year, hour, minute, second = match.groups()
        parsed = datetime(
            int(year),
            int(month),
            int(day),
            int(hour),
            int(minute),
            int(second or 0),
        )
        return parsed.isoformat()

    match = re.search(
        r"(\d{1,2}):(\d{2})\s*(am|pm),\s*PT,\s*([A-Za-z]+ \d{1,2}, \d{4})",
        value,
        flags=re.IGNORECASE,
    )
    if match:
        hour, minute, am_pm, date_text = match.groups()
        parsed = datetime.strptime(
            f"{date_text} {hour}:{minute} {am_pm.upper()}",
            "%B %d, %Y %I:%M %p",
        )
        return parsed.isoformat()

    match = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", value)
    if match:
        parsed = datetime.strptime(match.group(1), "%B %d, %Y")
        return parsed.date().isoformat()

    return value


def fetch_page(page):
    response = requests.get(
        TAG_URL,
        params={"pagina": page},
        headers=HEADERS,
        timeout=30,
    )
    logging.info(
        "Fetched page %s: status=%s bytes=%s",
        page,
        response.status_code,
        len(response.text),
    )

    if response.status_code == 404:
        return None

    response.raise_for_status()
    return response.text


def fetch_venturebeat_page(page):
    if page > 1:
        return None

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
        driver.get(VENTUREBEAT_AI_URL)
        WebDriverWait(driver, 20).until(
            lambda browser: browser.find_elements(By.CSS_SELECTOR, "article")
        )

        previous_count = 0
        stable_scrolls = 0
        for _ in range(VENTUREBEAT_SCROLL_STEPS):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)

            article_count = len(driver.find_elements(By.CSS_SELECTOR, "article"))
            if article_count == previous_count:
                stable_scrolls += 1
            else:
                stable_scrolls = 0

            previous_count = article_count
            if stable_scrolls >= 3:
                break

        html = driver.page_source
        logging.info(
            "Fetched VentureBeat rendered page: articles=%s bytes=%s",
            previous_count,
            len(html),
        )
        return html
    finally:
        driver.quit()


def fetch_url(url):
    max_retries = 3
    backoff = 1
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            logging.info(
                "Fetched article: status=%s bytes=%s url=%s",
                response.status_code,
                len(response.text),
                url,
            )
            response.raise_for_status()
            return response.text
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429 and attempt < max_retries:
                logging.warning("429 Too Many Requests for %s, retrying in %s seconds", url, backoff)
                time.sleep(backoff)
                backoff *= 2
            else:
                raise
        except Exception:
            if attempt < max_retries:
                logging.warning("Error fetching %s, retrying in %s seconds", url, backoff)
                time.sleep(backoff)
                backoff *= 2
            else:
                raise


def should_skip_paragraph(text):
    if not text or len(text) < MIN_PARAGRAPH_LENGTH:
        return True

    lower_text = text.lower()
    return any(pattern in lower_text for pattern in SKIP_PARAGRAPH_PATTERNS)


def meta_content(soup, *selectors):
    for selector in selectors:
        element = soup.select_one(selector)
        if element and element.get("content"):
            return clean_text(element["content"])
    return None


def json_ld_values(soup, key):
    values = []

    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or "")
        except json.JSONDecodeError:
            continue

        stack = data if isinstance(data, list) else [data]
        while stack:
            item = stack.pop()
            if isinstance(item, dict):
                if key in item:
                    values.append(item[key])
                stack.extend(item.values())
            elif isinstance(item, list):
                stack.extend(item)

    return [value for value in values if isinstance(value, str)]


def extract_published_date(soup):
    published_at = meta_content(
        soup,
        'meta[property="article:published_time"]',
        'meta[name="article:published_time"]',
        'meta[property="og:published_time"]',
        'meta[name="pubdate"]',
        'meta[itemprop="datePublished"]',
    )

    if not published_at:
        json_dates = json_ld_values(soup, "datePublished")
        published_at = clean_text(json_dates[0]) if json_dates else None

    if not published_at:
        time_element = soup.select_one("time[datetime]")
        published_at = clean_text(time_element.get("datetime")) if time_element else None

    if not published_at:
        published_element = soup.select_one(".published, .date, .detail-meta .published")
        published_at = clean_text(published_element.get_text(" ", strip=True)) if published_element else None

    return published_at


def extract_article_details(html):
    soup = BeautifulSoup(html, "html.parser")
    published_at = extract_published_date(soup)

    for unwanted in soup.select(
        "script, style, nav, header, footer, aside, form, button, iframe, "
        ".pub-container, .share, .social, .newsletter, .related, .tags"
    ):
        unwanted.decompose()

    container = None
    for selector in [
        ".entry-content",
        ".td-post-content",
        ".post-content",
        ".article-content",
        ".article-body",
        ".detail-body",
        ".single-content",
        "main article",
        "article",
        "main",
    ]:
        candidate = soup.select_one(selector)
        if candidate and len(candidate.get_text(" ", strip=True)) > 500:
            container = candidate
            break

    if not container:
        container = soup.body

    paragraphs = []
    seen = set()

    for paragraph in container.select("p"):
        text = clean_text(paragraph.get_text(" ", strip=True))
        if should_skip_paragraph(text) or text in seen:
            continue

        seen.add(text)
        paragraphs.append(text)

    description = meta_content(
        soup,
        'meta[name="description"]',
        'meta[property="og:description"]',
    )

    content_source = "article_paragraphs" if paragraphs else None
    if not paragraphs and description:
        paragraphs = [description]
        content_source = "meta_description"

    content_text = " ".join(paragraphs)
    if not description and paragraphs:
        description = paragraphs[0]

    return {
        "description": description,
        "published_at": parse_published_datetime(published_at),
        "_content": paragraphs or None,
        "_content_source": content_source,
        "_word_count": len(re.findall(r"\w+", content_text, flags=re.UNICODE)),
    }


def fetch_dynamic_article_details(url):
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
        driver.get(url)
        WebDriverWait(driver, 20).until(
            lambda browser: browser.find_elements(By.CSS_SELECTOR, "article, main")
        )

        text = driver.execute_script(
            """
            const parts = [];
            const lead = document.querySelector('article.detail-post .headline-lead');
            if (lead && lead.innerText.trim().length > 80) {
                parts.push(lead.innerText.trim());
            }

            const detail = document.querySelector('article.detail-post .detail-content');
            if (detail) {
                for (const element of Array.from(detail.children)) {
                    if (!['P', 'H2', 'H3', 'H4', 'LI', 'BLOCKQUOTE'].includes(element.tagName)) {
                        continue;
                    }
                    const value = (element.innerText || '').trim();
                    if (value.length > 80) parts.push(value);
                }
            }

            if (parts.length === 0) {
                const selectors = [
                    'article .entry-content p',
                    'article .td-post-content p',
                    'article .post-content p',
                    'article .article-content p'
                ];

                for (const selector of selectors) {
                    for (const element of document.querySelectorAll(selector)) {
                        const value = (element.innerText || '').trim();
                        if (value.length > 80) parts.push(value);
                    }
                }
            }

            return parts.join('\\n\\n');
            """
        )

        paragraphs = []
        seen = set()
        for line in (text or "").splitlines():
            line = clean_text(line)
            if should_skip_paragraph(line) or line in seen:
                continue
            seen.add(line)
            paragraphs.append(line)

        if not paragraphs:
            return None

        content_text = " ".join(paragraphs)
        details = extract_article_details(driver.page_source)
        details.update({
            "description": paragraphs[0],
            "_content": paragraphs,
            "_content_source": "dynamic_rendered",
            "_word_count": len(re.findall(r"\w+", content_text, flags=re.UNICODE)),
        })
        return details
    finally:
        driver.quit()


def enrich_article_content(article):
    try:
        details = extract_article_details(fetch_url(article["url"]))
        if (
            details["_content_source"] == "meta_description"
            or details["_word_count"] < DYNAMIC_FALLBACK_WORD_LIMIT
        ):
            dynamic_details = fetch_dynamic_article_details(article["url"])
            if dynamic_details and dynamic_details["_word_count"] > details["_word_count"]:
                details = dynamic_details

        article.update(details)
    except Exception:
        logging.exception("Erro ao extrair conteudo do artigo: %s", article["url"])
        article["description"] = None
        article["published_at"] = None
        article["_content"] = None
        article["_content_source"] = None
        article["_word_count"] = 0

    return article


def parse_article(article, page, scraped_at):
    title_link = article.select_one("h3.heading-default a[href]")
    picture_link = article.select_one("a.picture[href]")
    image = article.select_one("img")
    partner = article.get("data-partner") or clean_text(
        article.select_one(".partner span").get_text(" ", strip=True)
        if article.select_one(".partner span")
        else None
    )

    title = clean_text(title_link.get_text(" ", strip=True) if title_link else None)
    if not title and image:
        title = clean_text(image.get("alt"))

    link = title_link.get("href") if title_link else None
    if not link and picture_link:
        link = picture_link.get("href")

    if not title or not link:
        return None

    url = absolute_url(link)
    source = "sapo"

    return {
        "id": make_article_id(source, url, article.get("data-article-id")),
        "title": title,
        "url": url,
        "source": source,
        "description": None,
        "published_at": None,
        "scraped_at": scraped_at,
    }


def parse_page(html, page, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    articles = []

    for article in soup.select("article.article-default"):
        try:
            parsed = parse_article(article, page, scraped_at)
            if parsed:
                articles.append(parsed)
        except Exception:
            logging.exception("Erro ao processar artigo na pagina %s", page)

    return articles


def parse_venturebeat_page(html, page, scraped_at):
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    seen_urls = set()

    for article in soup.select("article"):
        source = "venturebeat"
        title_element = article.select_one("h2 a[href], h3 a[href], h2, h3")
        link = title_element if title_element and title_element.name == "a" else None

        if not link and title_element:
            link = title_element.select_one("a[href]")

        if not link:
            link = article.select_one('a[href*="venturebeat.com/"], a[href^="/"]')

        title = clean_text(title_element.get_text(" ", strip=True) if title_element else None)
        if not title and link:
            title = clean_text(link.get_text(" ", strip=True))

        url = absolute_url(link.get("href"), VENTUREBEAT_BASE_URL) if link else None

        if not title or not url or not url.startswith(VENTUREBEAT_BASE_URL):
            continue

        if (
            "/category/" in url
            or "/author/" in url
            or "/tag/" in url
            or url.rstrip("/") == VENTUREBEAT_BASE_URL
            or url in seen_urls
        ):
            continue

        seen_urls.add(url)
        image = article.select_one("img")
        description = clean_text(
            article.select_one("p").get_text(" ", strip=True)
            if article.select_one("p")
            else None
        )
        if not description and image:
            description = clean_text(image.get("alt"))

        articles.append({
            "id": make_article_id(source, url),
            "title": title,
            "url": url,
            "source": source,
            "description": description,
            "published_at": None,
            "scraped_at": scraped_at,
        })

    return articles


def collect_source_articles(source_name, fetch_page_func, parse_page_func, scraped_at, max_pages):
    listed_articles = []
    seen_urls = set()

    for page in range(1, max_pages + 1):
        html = fetch_page_func(page)
        if html is None:
            logging.info("%s page %s returned 404; stopping", source_name, page)
            break

        page_articles = parse_page_func(html, page, scraped_at)
        new_articles = [
            article for article in page_articles
            if article["url"] not in seen_urls
        ]

        for article in new_articles:
            seen_urls.add(article["url"])

        logging.info(
            "%s page %s: %s articles, %s new",
            source_name,
            page,
            len(page_articles),
            len(new_articles),
        )

        if not new_articles:
            break

        listed_articles.extend(new_articles)

    return listed_articles


OUTPUT_FIELDS = [
    "id",
    "title",
    "url",
    "source",
    "description",
    "published_at",
    "scraped_at",
]


def normalize_source(source):
    source = clean_text(source or "")
    if not source:
        return None

    return re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")


def source_from_url(url):
    if not url:
        return None
    if url.startswith(VENTUREBEAT_BASE_URL):
        return "venturebeat"
    if url.startswith(BASE_URL):
        return "sapo"
    return None


def normalize_article(article):
    url = article.get("url")
    source = source_from_url(url) or normalize_source(article.get("source"))
    description = article.get("description")

    if not description and isinstance(article.get("content"), list):
        description = article["content"][0] if article["content"] else None

    published_at = article.get("published_at")
    if not published_at:
        published_at = parse_published_datetime(article.get("published_at_raw"))

    normalized = {
        "id": article.get("id") or make_article_id(source or "news", url or ""),
        "title": article.get("title"),
        "url": url,
        "source": source,
        "description": description,
        "published_at": published_at,
        "scraped_at": article.get("scraped_at"),
    }

    return {field: normalized.get(field) for field in OUTPUT_FIELDS}


def has_output_schema(article):
    return list(article.keys()) == OUTPUT_FIELDS


def scrape(max_pages=MAX_PAGES):
    logging.info("Start scraping AI news")

    scraped_at = datetime.now(timezone.utc).isoformat()
    existing_articles = load_json()
    existing_by_url = {
        article["url"]: article
        for article in existing_articles
        if article.get("url")
    }
    existing_urls = [article.get("url") for article in existing_articles]
    listed_articles = []
    listed_articles.extend(
        collect_source_articles("SAPO", fetch_page, parse_page, scraped_at, max_pages)
    )
    listed_articles.extend(
        collect_source_articles(
            "VentureBeat",
            fetch_venturebeat_page,
            parse_venturebeat_page,
            scraped_at,
            max_pages,
        )
    )

    listed_urls = [article["url"] for article in listed_articles]
    if listed_urls == existing_urls:
        if not all(has_output_schema(article) for article in existing_articles):
            normalized_articles = [normalize_article(article) for article in existing_articles]
            save_json(normalized_articles)
            logging.info("JSON convertido para a nova estrutura com %s noticias", len(normalized_articles))
            return normalized_articles

        logging.info("Sem alteracoes nas noticias; JSON mantido com %s noticias", len(existing_articles))
        return existing_articles

    articles = []
    new_count = 0
    reused_count = 0

    for article in listed_articles:
        existing_article = existing_by_url.get(article["url"])
        if existing_article:
            merged_article = normalize_article(existing_article)
            merged_article.update({
                "id": article["id"],
                "title": article["title"],
                "source": article["source"],
                "url": article["url"],
                "description": merged_article["description"] or article["description"],
                "published_at": merged_article["published_at"] or article["published_at"],
                "scraped_at": article["scraped_at"],
            })
            articles.append(merged_article)
            reused_count += 1
        else:
            articles.append(normalize_article(enrich_article_content(article)))
            new_count += 1
            time.sleep(1)  # Delay to avoid rate limiting

    articles = [normalize_article(article) for article in articles]
    save_json(articles)
    logging.info(
        "%s noticias guardadas; %s novas, %s reutilizadas",
        len(articles),
        new_count,
        reused_count,
    )
    return articles


def load_json():
    if not os.path.exists(OUTPUT_FILE):
        return []

    with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
        return json.load(file)


def save_json(data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    scrape()
