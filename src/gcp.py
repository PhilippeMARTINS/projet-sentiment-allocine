"""
gcp.py
------
Module GCP : upload vers Cloud Storage et chargement dans BigQuery.
"""

import pandas as pd
from google.cloud import storage, bigquery
from pathlib import Path


PROJECT_ID  = "project-b4d81ada-e874-4f3f-82e"
BUCKET_NAME = "sentiment-allocine-bucket"
DATASET_ID  = "sentiment_allocine"
TABLE_ID    = "reviews"

PROCESSED_PATH = Path("data/processed")


def upload_to_gcs(local_path: Path, blob_name: str) -> str:
    """
    Upload un fichier local vers Google Cloud Storage.

    Args:
        local_path: Chemin local du fichier
        blob_name: Nom du fichier dans le bucket

    Returns:
        str: URI GCS du fichier uploadé
    """
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    blob   = bucket.blob(blob_name)

    blob.upload_from_filename(str(local_path))
    uri = f"gs://{BUCKET_NAME}/{blob_name}"
    print(f"✅ Fichier uploadé sur GCS : {uri}")
    return uri


def load_to_bigquery(df: pd.DataFrame) -> None:
    """
    Charge un DataFrame dans BigQuery.

    Args:
        df: DataFrame à charger
    """
    client    = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("content_id",            "STRING"),
            bigquery.SchemaField("content_type",          "STRING"),
            bigquery.SchemaField("content_name",          "STRING"),
            bigquery.SchemaField("titre_avis",            "STRING"),
            bigquery.SchemaField("note",                  "FLOAT"),
            bigquery.SchemaField("texte",                 "STRING"),
            bigquery.SchemaField("date_clean",            "STRING"),
            bigquery.SchemaField("sentiment_label",       "STRING"),
            bigquery.SchemaField("sentiment_score",       "INTEGER"),
            bigquery.SchemaField("sentiment_confidence",  "FLOAT"),
            bigquery.SchemaField("coherence",             "STRING"),
            bigquery.SchemaField("scraped_at",            "STRING"),
        ],
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ {len(df)} lignes chargées dans BigQuery : {table_ref}")


def run_gcp_pipeline() -> None:
    """
    Orchestre l'upload GCS et le chargement BigQuery.
    """
    print("=== CHARGEMENT GCP ===")

    # Chargement du fichier local
    csv_path = PROCESSED_PATH / "reviews_clean.csv"
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    print(f"✅ {len(df)} avis chargés depuis {csv_path}")

    # Upload vers GCS
    upload_to_gcs(csv_path, "reviews_clean.csv")

    # Conversion des types avant chargement BigQuery
    df["content_id"] = df["content_id"].astype(str)
    df["sentiment_score"] = df["sentiment_score"].astype(int)

    # Chargement dans BigQuery
    load_to_bigquery(df)

    print("\n✅ Pipeline GCP terminé !")


if __name__ == "__main__":
    run_gcp_pipeline()