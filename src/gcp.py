"""
gcp.py
------
Module GCP : upload vers Cloud Storage et chargement dans BigQuery.
Les credentials sont lus depuis le fichier .env (voir .env.example).
"""

import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage


# ── Configuration ─────────────────────────────────────────────────────────────
load_dotenv()

logger = logging.getLogger(__name__)

PROJECT_ID  = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DATASET_ID  = os.getenv("DATASET_ID")
TABLE_ID    = os.getenv("TABLE_ID")

PROCESSED_PATH = Path("data/processed")


def _check_env() -> None:
    """
    Vérifie que toutes les variables d'environnement GCP sont définies.

    Raises:
        EnvironmentError: Si une variable est manquante dans .env
    """
    manquantes = [
        var for var, val in {
            "PROJECT_ID":  PROJECT_ID,
            "BUCKET_NAME": BUCKET_NAME,
            "DATASET_ID":  DATASET_ID,
            "TABLE_ID":    TABLE_ID,
        }.items()
        if not val
    ]
    if manquantes:
        raise EnvironmentError(
            f"Variables d'environnement GCP manquantes dans .env : {manquantes}"
        )


def upload_to_gcs(local_path: Path, blob_name: str) -> str:
    """
    Upload un fichier local vers Google Cloud Storage.

    Args:
        local_path: Chemin local du fichier
        blob_name:  Nom du fichier dans le bucket

    Returns:
        str: URI GCS du fichier uploadé (gs://...)
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)

    blob.upload_from_filename(str(local_path))
    uri = f"gs://{BUCKET_NAME}/{blob_name}"
    logger.info("Fichier uploadé sur GCS : %s", uri)
    return uri


def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Charge un DataFrame dans la table BigQuery définie dans .env.

    Args:
        df: DataFrame à charger (doit correspondre au schéma défini)
    """
    client    = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("content_id",           "STRING"),
            bigquery.SchemaField("content_type",         "STRING"),
            bigquery.SchemaField("content_name",         "STRING"),
            bigquery.SchemaField("titre_avis",           "STRING"),
            bigquery.SchemaField("note",                 "FLOAT"),
            bigquery.SchemaField("texte",                "STRING"),
            bigquery.SchemaField("date_clean",           "STRING"),
            bigquery.SchemaField("sentiment_label",      "STRING"),
            bigquery.SchemaField("sentiment_score",      "INTEGER"),
            bigquery.SchemaField("sentiment_confidence", "FLOAT"),
            bigquery.SchemaField("coherence",            "STRING"),
            bigquery.SchemaField("scraped_at",           "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info("%d lignes chargées dans BigQuery : %s", len(df), table_ref)


def run_gcp_pipeline() -> None:
    """
    Orchestre la vérification des variables d'env, l'upload GCS
    et le chargement BigQuery.

    Raises:
        EnvironmentError: Si les variables GCP sont manquantes
        FileNotFoundError: Si le fichier reviews_clean.csv est absent
    """
    _check_env()

    csv_path = PROCESSED_PATH / "reviews_clean.csv"
    if not csv_path.exists():
        logger.error("Fichier introuvable : %s — lancer d'abord transform.py", csv_path)
        raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info("%d avis chargés depuis '%s'", len(df), csv_path)

    upload_to_gcs(csv_path, "reviews_clean.csv")

    df["content_id"]      = df["content_id"].astype(str)
    df["sentiment_score"] = df["sentiment_score"].astype(int)

    load_to_bigquery(df)
    logger.info("Pipeline GCP terminé")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_gcp_pipeline()