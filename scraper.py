import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime
import logging
import os

os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/imdb.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

URL = "https://www.imdb.com/chart/top"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def scrape():
    logging.info("Start scraping")

    response = requests.get(URL, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")

    movies = []

    for item in soup.select(".ipc-metadata-list-summary-item"):
        try:
            title = item.select_one("h3").text.strip()
            rating = float(item.select_one(".ipc-rating-star").text.split()[0])

            movies.append({
                "title": title,
                "source": "imdb",
                "rating": rating,
                "votes": None,
                "genre": None,
                "timestamp": datetime.utcnow().isoformat()
            })

        except:
            logging.error("Erro parsing")

    save_json(movies)
    logging.info(f"{len(movies)} filmes guardados")

def save_json(data):
    path = "data/imdb.json"

    if os.path.exists(path):
        with open(path, "r") as f:
            existing = json.load(f)
    else:
        existing = []

    existing.extend(data)

    with open(path, "w") as f:
        json.dump(existing, f, indent=2)

if __name__ == "__main__":
    scrape()