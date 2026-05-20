"""
main.py
-------
Point d'entrée du pipeline complet — Sentiment Allociné.
Scrape → Transform → GCP (optionnel) → Analyze
"""

import logging
from dotenv import dotenv_values
from src.scraper import run_scraping
from src.transform import run_transformations
from src.gcp import run_gcp_pipeline
from src.analyze import run_analysis


# ── Configuration du logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),                         # affichage console
        logging.FileHandler("pipeline.log", mode="w"),  # sauvegarde fichier
    ],
)

logger = logging.getLogger(__name__)


def gcp_est_configure() -> bool:
    """
    Vérifie que les variables GCP essentielles sont renseignées dans .env.

    Returns:
        bool: True si PROJECT_ID, BUCKET_NAME et DATASET_ID sont définis
    """
    env = dotenv_values(".env")
    variables_requises = ["PROJECT_ID", "BUCKET_NAME", "DATASET_ID"]
    return all(env.get(var, "").strip() for var in variables_requises)


if __name__ == "__main__":
    logger.info("=" * 55)
    logger.info("  PIPELINE SENTIMENT ALLOCINE")
    logger.info("=" * 55)

    logger.info("ÉTAPE 1 — SCRAPING ALLOCINÉ")
    df_raw = run_scraping()
    if df_raw.empty:
        logger.error("Aucun avis récupéré — arrêt du pipeline")
        raise SystemExit(1)
    logger.info("%d avis récupérés", len(df_raw))

    logger.info("ÉTAPE 2 — TRANSFORMATION & ANALYSE DE SENTIMENT")
    run_transformations()

    logger.info("ÉTAPE 3 — CHARGEMENT GCP")
    if gcp_est_configure():
        run_gcp_pipeline()
    else:
        logger.warning(
            "ÉTAPE 3 ignorée — fichier .env incomplet "
            "(PROJECT_ID, BUCKET_NAME ou DATASET_ID manquant). "
            "Renseigner ces variables dans .env pour activer cette étape."
        )

    logger.info("ÉTAPE 4 — VISUALISATIONS")
    run_analysis()

    logger.info("=" * 55)
    logger.info("PIPELINE TERMINÉ")
    logger.info("=" * 55)