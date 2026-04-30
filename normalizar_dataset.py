import json
from pathlib import Path
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "dataset_final.json"

INPUT_FILES = {
    "carlota": RAW_DIR / "carlota.json",
    "rodrigo": RAW_DIR / "rodrigo.json",
    "tiago": RAW_DIR / "tiago.json",
}

SOURCE_DOMAINS = {
    "sapo": "sapo.pt",
    "towardsdatascience": "towardsdatascience.com",
}

DATE_FORMATS = [
    "%B %d, %Y",
    "%b %d, %Y",
]


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def normalize_source(source):
    if isinstance(source, dict):
        name = source.get("name")
        domain = source.get("domain")

        if domain == "artificialintelligence-news.com":
            name = "AI News"

        return {
            "name": name,
            "domain": domain,
        }

    return {
        "name": source,
        "domain": SOURCE_DOMAINS.get(source),
    }


def normalize_published_at(value):
    if not value:
        return None

    value = str(value).strip()

    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(value, date_format).date().isoformat()
        except ValueError:
            pass

    return value


def normalize_carlota(article):
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "content": article.get("description"),
        "source": normalize_source(article.get("source")),
        "url": article.get("url"),
        "published_at": normalize_published_at(article.get("published_at")),
        "collected_at": article.get("scraped_at"),
        "author": None,
    }


def normalize_default(article):
    return {
        "id": article.get("id"),
        "title": article.get("title"),
        "content": article.get("content"),
        "source": normalize_source(article.get("source")),
        "url": article.get("url"),
        "published_at": normalize_published_at(article.get("published_at")),
        "collected_at": article.get("collected_at"),
        "author": article.get("author"),
    }


def normalize_article(owner, article):
    if owner == "carlota":
        return normalize_carlota(article)

    return normalize_default(article)


def build_dataset():
    dataset = []
    seen_urls = set()

    for owner, path in INPUT_FILES.items():
        articles = load_json(path)

        for article in articles:
            normalized = normalize_article(owner, article)
            url = normalized.get("url")

            if not url or url in seen_urls:
                continue

            seen_urls.add(url)
            dataset.append(normalized)

    return dataset


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    dataset = build_dataset()

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(dataset, file, indent=2, ensure_ascii=False)

    print(f"Dataset final criado em: {OUTPUT_FILE}")
    print(f"Total de artigos: {len(dataset)}")


if __name__ == "__main__":
    main()
