import json
from pathlib import Path
from datetime import datetime


base_dir = Path(__file__).resolve().parent
raw_dir =  base_dir / "data" / "raw"
processed_dir = base_dir/ "data" / "processed"
output_file = processed_dir / "dataset_final.json"

input_files = {
    "carlota": raw_dir / "carlota.json",
    "rodrigo": raw_dir / "rodrigo.json",
    "tiago": raw_dir / "tiago.json",
}

source_domains = {"sapo": "sapo.pt","towardsdatascience": "towardsdatascience.com"}

date_formats = ["%B %d, %Y","%b %d, %Y"]


def load_json(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def normalize_source(source):
    if isinstance(source, dict):
        name = source.get("name")
        domain = source.get("domain")

        if domain == "artificialintelligence-news.com":
            name = "AI News"

        return {"name": name,"domain": domain}

    return {"name": source,"domain": source_domains.get(source)}


def normalize_published_at(value):
    if not value:
        return None

    value = str(value).strip()

    if len(value) >= 10 and value[4] == "-" and value[7] == "-":
        return value[:10]

    for date_format in date_formats:
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

    for owner, path in input_files.items():
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
    processed_dir.mkdir(parents=True, exist_ok=True)

    dataset = build_dataset()

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(dataset, file, indent=2, ensure_ascii=False)

    print(f"Dataset final criado em: {output_file}")
    print(f"Total de artigos: {len(dataset)}")


if __name__ == "__main__":
    main()
