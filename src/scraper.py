"""
scraper.py
----------
Module de scraping : récupération des avis Allociné (films + séries).
Utilise Selenium + BeautifulSoup pour les pages JavaScript dynamiques.
"""

import logging
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager


# ── Configuration ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

RAW_DATA_PATH = Path("data/raw")
RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://www.allocine.fr"

# Films et séries avec leurs vrais IDs Allociné
CONTENUS = [
    ("182745", "film",   "Intouchables"),
    ("143692", "film",   "Inception"),
    ("27063",  "film",   "Amelie Poulain"),
    ("115362", "film",   "The Dark Knight"),
    ("61282",  "film",   "Interstellar"),
    ("3517",   "series", "Breaking Bad"),
    ("604",    "series", "Game of Thrones"),
    ("19156",  "series", "Stranger Things"),
    ("21504",  "series", "La Casa de Papel"),
    ("7330",   "series", "The Walking Dead"),
]


def create_driver() -> webdriver.Chrome:
    """
    Crée et configure le driver Selenium Chrome en mode headless.

    Returns:
        webdriver.Chrome: Driver configuré
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--lang=fr-FR")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    logger.debug("Driver Selenium initialisé")
    return driver


def get_page_source(driver: webdriver.Chrome, url: str) -> Optional[BeautifulSoup]:
    """
    Charge une page avec Selenium et retourne le HTML parsé.

    Args:
        driver: Driver Selenium actif
        url:    URL à charger

    Returns:
        Optional[BeautifulSoup]: HTML parsé ou None si erreur critique
    """
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "review-card"))
        )
        time.sleep(random.uniform(1.0, 2.0))
        return BeautifulSoup(driver.page_source, "html.parser")

    except Exception as e:
        logger.warning("Erreur chargement %s : %s", url, e)
        try:
            return BeautifulSoup(driver.page_source, "html.parser")
        except Exception:
            return None


def extract_reviews_from_soup(
    soup: BeautifulSoup,
    content_id: str,
    content_type: str,
    content_name: str,
) -> list[dict]:
    """
    Extrait tous les avis d'une page HTML parsée.

    Args:
        soup:         HTML parsé de la page
        content_id:   ID Allociné du contenu
        content_type: 'film' ou 'series'
        content_name: Nom du film/série

    Returns:
        list[dict]: Liste des avis extraits
    """
    reviews = []

    blocks = (
        soup.find_all("div", class_="review-card")
        or soup.find_all("div", class_="hred review-card")
        or soup.find_all("div", attrs={"data-testid": "review-card"})
    )

    for block in blocks:
        try:
            # Note
            note_tag = (
                block.find("span", class_="stareval-note")
                or block.find("span", class_="rating-mdl")
            )
            note = None
            if note_tag:
                note = float(note_tag.get_text(strip=True).replace(",", "."))

            # Texte
            texte_tag = (
                block.find("div", class_="content-txt")
                or block.find("p",   class_="review-card-content")
                or block.find("div", class_="review-card-review-holder")
            )
            texte = texte_tag.get_text(strip=True) if texte_tag else None

            # Titre
            titre_tag = block.find("strong", class_="review-card-title")
            titre = titre_tag.get_text(strip=True) if titre_tag else None

            # Date
            date_tag = block.find("span", class_="review-card-meta-date")
            date = date_tag.get_text(strip=True) if date_tag else None

            if not texte:
                continue

            reviews.append({
                "content_id":   content_id,
                "content_type": content_type,
                "content_name": content_name,
                "titre_avis":   titre,
                "note":         note,
                "texte":        texte,
                "date":         date,
                "scraped_at":   datetime.now().isoformat(),
            })

        except Exception as e:
            logger.warning("Erreur extraction d'un bloc avis : %s", e)
            continue

    return reviews


def scrape_content(
    driver: webdriver.Chrome,
    content_id: str,
    content_type: str,
    content_name: str,
    n_pages: int = 10,
) -> list[dict]:
    """
    Scrape les avis d'un film ou d'une série sur n pages.

    Args:
        driver:       Driver Selenium actif
        content_id:   ID Allociné
        content_type: 'film' ou 'series'
        content_name: Nom du contenu
        n_pages:      Nombre de pages à scraper

    Returns:
        list[dict]: Tous les avis récupérés
    """
    all_reviews = []

    for page in range(1, n_pages + 1):
        if content_type == "film":
            url = f"{BASE_URL}/film/fichefilm-{content_id}/critiques/spectateurs/?page={page}"
        else:
            url = f"{BASE_URL}/series/ficheserie-{content_id}/critiques/?page={page}"

        soup = get_page_source(driver, url)
        if not soup:
            break

        reviews = extract_reviews_from_soup(soup, content_id, content_type, content_name)

        if not reviews:
            logger.debug("Aucun avis page %d pour '%s' — arrêt", page, content_name)
            break

        all_reviews.extend(reviews)
        logger.debug("  Page %d/%d — %d avis récupérés", page, n_pages, len(reviews))
        time.sleep(random.uniform(1.5, 2.5))

    return all_reviews


def run_scraping() -> pd.DataFrame:
    """
    Lance le scraping complet sur tous les contenus définis dans CONTENUS.

    Returns:
        pd.DataFrame: Tous les avis récupérés, sauvegardés dans data/raw/reviews_raw.csv
    """
    logger.info("Scraping Allociné — %d contenus à traiter", len(CONTENUS))

    driver = create_driver()
    all_reviews = []

    try:
        for content_id, content_type, content_name in CONTENUS:
            logger.info("Scraping : %s (%s)", content_name, content_type)
            reviews = scrape_content(driver, content_id, content_type, content_name)
            all_reviews.extend(reviews)
            logger.info("  %d avis récupérés", len(reviews))
            time.sleep(random.uniform(2.0, 3.0))

    finally:
        driver.quit()
        logger.debug("Driver Selenium fermé")

    df = pd.DataFrame(all_reviews)

    if df.empty:
        logger.warning("Aucun avis récupéré — vérifier les sélecteurs CSS ou l'accès au site")
        return df

    output_path = RAW_DATA_PATH / "reviews_raw.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    nb_films  = len(df[df["content_type"] == "film"])
    nb_series = len(df[df["content_type"] == "series"])
    logger.info(
        "%d avis sauvegardés dans '%s' (films: %d, séries: %d, note moyenne: %.2f/5)",
        len(df), output_path, nb_films, nb_series, df["note"].mean(),
    )

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    df = run_scraping()
    if not df.empty:
        logger.info("Aperçu :\n%s", df.head().to_string())