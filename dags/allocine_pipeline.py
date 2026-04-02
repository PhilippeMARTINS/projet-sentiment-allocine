"""
allocine_pipeline.py
--------------------
DAG Airflow — Pipeline automatisé Allociné.
Orchestration : Scraping -> Transform -> GCP -> Analyze
Schedule : tous les lundis à 8h00
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Ajout du répertoire projet au PATH pour les imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scraper import run_scraping
from src.transform import run_transformations
from src.gcp import run_gcp_pipeline
from src.analyze import run_analysis


# ── Configuration du DAG ───────────────────────────────────────────────────────
default_args = {
    "owner":            "philippe.martins",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="allocine_sentiment_pipeline",
    default_args=default_args,
    description="Pipeline ETL complet : scraping Allociné + sentiment NLP + GCP",
    schedule_interval="0 8 * * 1",  # Tous les lundis à 8h00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["allocine", "nlp", "sentiment", "gcp"],
)


# ── Tâches ─────────────────────────────────────────────────────────────────────
task_scraping = PythonOperator(
    task_id="scraping_allocine",
    python_callable=run_scraping,
    dag=dag,
)

task_transform = PythonOperator(
    task_id="transformation_sentiment",
    python_callable=run_transformations,
    dag=dag,
)

task_gcp = PythonOperator(
    task_id="chargement_gcp",
    python_callable=run_gcp_pipeline,
    dag=dag,
)

task_analyze = PythonOperator(
    task_id="analyse_visualisation",
    python_callable=run_analysis,
    dag=dag,
)


# ── Dépendances (ordre d'exécution) ───────────────────────────────────────────
task_scraping >> task_transform >> task_gcp >> task_analyze