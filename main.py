"""
main.py
-------
Point d'entrée du pipeline complet :
Scrape -> Transform -> GCP -> Analyze
"""

from src.scraper import run_scraping
from src.transform import run_transformations
from src.gcp import run_gcp_pipeline
from src.analyze import run_analysis


if __name__ == "__main__":
    print("=" * 55)
    print("  PIPELINE SENTIMENT ALLOCINE")
    print("=" * 55)

    print("\n=== ETAPE 1 — SCRAPING ALLOCINE ===")
    run_scraping()

    print("\n=== ETAPE 2 — TRANSFORMATION & SENTIMENT ===")
    run_transformations()

    print("\n=== ETAPE 3 — CHARGEMENT GCP ===")
    run_gcp_pipeline()

    print("\n=== ETAPE 4 — VISUALISATIONS ===")
    run_analysis()

    print("\n" + "=" * 55)
    print("  PIPELINE TERMINE")
    print("=" * 55)