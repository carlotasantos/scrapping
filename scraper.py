import json
import logging
import os
import re
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
OUTPUT_FILE = os.path.join(DATA_DIR, "ai_news.json")
MAX_PAGES = 10
MIN_PARAGRAPH_LENGTH = 40
DYNAMIC_FALLBACK_WORD_LIMIT = 80

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


def absolute_url(value):
    if not value:
        return None
    return urljoin(BASE_URL, value)


def format_published_datetime(value):
    if not value:
        return None, None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.strftime("%d/%m/%Y"), parsed.strftime("%H:%M:%S")
    except ValueError:
        pass

    match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4}).*?(\d{1,2}):(\d{2})(?::(\d{2}))?", value)
    if match:
        day, month, year, hour, minute, second = match.groups()
        return f"{int(day):02d}/{int(month):02d}/{year}", f"{int(hour):02d}:{minute}:{second or '00'}"

    return value, None


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


def fetch_url(url):
    response = requests.get(url, headers=HEADERS, timeout=30)
    logging.info(
        "Fetched article: status=%s bytes=%s url=%s",
        response.status_code,
        len(response.text),
        url,
    )
    response.raise_for_status()
    return response.text


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

    dia, horas = format_published_datetime(published_at)
    content_text = " ".join(paragraphs)

    return {
        "content": paragraphs or None,
        "content_source": content_source,
        "published_at_raw": published_at,
        "dia": dia,
        "horas": horas,
        "word_count": len(re.findall(r"\w+", content_text, flags=re.UNICODE)),
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

        details = extract_article_details(driver.page_source)
        dia, horas = format_published_datetime(details.get("published_at_raw"))
        content_text = " ".join(paragraphs)
        details.update({
            "content": paragraphs,
            "content_source": "dynamic_rendered",
            "dia": dia,
            "horas": horas,
            "word_count": len(re.findall(r"\w+", content_text, flags=re.UNICODE)),
        })
        return details
    finally:
        driver.quit()


def enrich_article_content(article):
    try:
        details = extract_article_details(fetch_url(article["url"]))
        if (
            details["content_source"] == "meta_description"
            or details["word_count"] < DYNAMIC_FALLBACK_WORD_LIMIT
        ):
            dynamic_details = fetch_dynamic_article_details(article["url"])
            if dynamic_details and dynamic_details["word_count"] > details["word_count"]:
                details = dynamic_details

        article.update(details)
    except Exception:
        logging.exception("Erro ao extrair conteudo do artigo: %s", article["url"])
        article["content"] = None
        article["content_source"] = None
        article["published_at_raw"] = None
        article["dia"] = None
        article["horas"] = None
        article["word_count"] = 0

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

    category = clean_text(
        article.select_one(".button-tag").get_text(" ", strip=True)
        if article.select_one(".button-tag")
        else None
    )

    return {
        "id": article.get("data-article-id"),
        "title": title,
        "source": partner,
        "category": category,
        "tag": "inteligencia-artificial",
        "url": absolute_url(link),
        "image_url": absolute_url(image.get("src")) if image else None,
        "page": page,
        "scraped_at": scraped_at,
        "dia": None,
        "horas": None,
        "published_at_raw": None,
        "content_source": None,
        "word_count": 0,
        "content": None,
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


def scrape(max_pages=MAX_PAGES):
    logging.info("Start scraping SAPO AI news")

    scraped_at = datetime.now(timezone.utc).isoformat()
    articles = []
    seen_urls = set()

    for page in range(1, max_pages + 1):
        html = fetch_page(page)
        if html is None:
            logging.info("Page %s returned 404; stopping", page)
            break

        page_articles = parse_page(html, page, scraped_at)
        new_articles = [
            article for article in page_articles
            if article["url"] not in seen_urls
        ]

        for article in new_articles:
            seen_urls.add(article["url"])

        logging.info(
            "Page %s: %s articles, %s new",
            page,
            len(page_articles),
            len(new_articles),
        )

        if not new_articles:
            break

        articles.extend(enrich_article_content(article) for article in new_articles)

    save_json(articles)
    logging.info("%s noticias guardadas", len(articles))
    return articles


def save_json(data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    scrape()
