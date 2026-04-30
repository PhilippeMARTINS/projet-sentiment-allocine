"""
main.py
-------
Point d'entrée du pipeline complet — Sentiment Allociné.
Scrape → Transform → GCP → Analyze
"""

import logging
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
    run_gcp_pipeline()

    logger.info("ÉTAPE 4 — VISUALISATIONS")
    run_analysis()

    logger.info("=" * 55)
    logger.info("PIPELINE TERMINÉ")
    logger.info("=" * 55)