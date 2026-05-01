"""
transform.py
------------
Module de transformation : nettoyage des avis et analyse de sentiment
avec le modèle HuggingFace 'nlptown/bert-base-multilingual-uncased-sentiment'.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd


# ── Configuration ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

RAW_DATA_PATH       = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

COLONNES_FINALES = [
    "content_id", "content_type", "content_name",
    "titre_avis", "note", "texte", "date_clean",
    "sentiment_label", "sentiment_score", "sentiment_confidence",
    "coherence", "scraped_at",
]


# ── Fonctions de nettoyage ─────────────────────────────────────────────────────

def clean_text(text: str) -> str:
    """
    Nettoie le texte brut d'un avis.

    Args:
        text: Texte brut issu du scraping

    Returns:
        str: Texte nettoyé (espaces normalisés, caractères spéciaux supprimés)
    """
    if not isinstance(text, str):
        return ""
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[^\w\s\.,!?;:'\"-]", " ", text)
    return text


def clean_date(date_str: str) -> Optional[str]:
    """
    Normalise la date au format YYYY-MM-DD.

    Args:
        date_str: Date brute (ex: 'Publiée le 17 octobre 2013')

    Returns:
        Optional[str]: Date normalisée ou None si format non reconnu
    """
    if not isinstance(date_str, str):
        return None

    mois = {
        "janvier": "01", "février": "02", "mars":     "03",
        "avril":   "04", "mai":     "05", "juin":     "06",
        "juillet": "07", "août":    "08", "septembre": "09",
        "octobre": "10", "novembre": "11", "décembre": "12",
    }

    try:
        parts = date_str.replace("Publiée le ", "").strip().split()
        if len(parts) == 3:
            jour, mois_str, annee = parts
            return f"{annee}-{mois.get(mois_str, '01')}-{jour.zfill(2)}"
    except Exception:
        pass
    return None


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie et type le DataFrame d'avis bruts.

    Args:
        df: DataFrame brut issu de run_scraping()

    Returns:
        pd.DataFrame: DataFrame nettoyé, sans lignes vides ou hors-plage
    """
    df = df.copy()
    df["texte"]      = df["texte"].apply(clean_text)
    df["date_clean"] = df["date"].apply(clean_date)
    df["note"]       = pd.to_numeric(df["note"], errors="coerce")

    nb_avant = len(df)
    df = df.dropna(subset=["texte", "note"])
    df = df[df["texte"].str.len() > 10]
    df = df[df["note"].between(0.5, 5.0)]
    nb_apres = len(df)

    logger.info(
        "Nettoyage : %d avis -> %d après suppression des lignes invalides (%d supprimées)",
        nb_avant, nb_apres, nb_avant - nb_apres,
    )
    return df


# ── Fonctions NLP ──────────────────────────────────────────────────────────────

def load_sentiment_model():
    """
    Charge le modèle de sentiment analysis multilingue depuis HuggingFace.

    Modèle : 'nlptown/bert-base-multilingual-uncased-sentiment'
    Supporte le français, retourne un score de 1 à 5 étoiles.

    Returns:
        pipeline: Modèle HuggingFace prêt à l'emploi
    """
    from transformers import pipeline as hf_pipeline
    logger.info("Chargement du modèle NLP HuggingFace (peut prendre quelques secondes)...")
    model = hf_pipeline(
        "sentiment-analysis",
        model="nlptown/bert-base-multilingual-uncased-sentiment",
        truncation=True,
        max_length=512,
    )
    logger.info("Modèle NLP chargé")
    return model


def predict_sentiment_batch(
    texts: list[str],
    model,
    batch_size: int = 32,
) -> list[dict]:
    """
    Prédit le sentiment pour une liste de textes, par batch.

    Args:
        texts:      Liste de textes à analyser
        model:      Modèle HuggingFace chargé par load_sentiment_model()
        batch_size: Nombre de textes traités par batch (défaut : 32)

    Returns:
        list[dict]: Liste de {'label': '4 stars', 'score': 0.87}
                    En cas d'erreur sur un batch, score 3 stars / confidence 0.0
    """
    results = []
    total = len(texts)

    for i in range(0, total, batch_size):
        batch = texts[i : i + batch_size]
        try:
            preds = model(batch)
            results.extend(preds)
        except Exception as e:
            logger.warning("Erreur sur le batch %d-%d : %s", i, i + batch_size, e)
            results.extend([{"label": "3 stars", "score": 0.0}] * len(batch))

        if (i // batch_size) % 5 == 0:
            logger.info(
                "Analyse sentiment : %d/%d avis traités",
                min(i + batch_size, total), total,
            )

    return results


def parse_sentiment_label(label: str) -> int:
    """
    Convertit un label HuggingFace en entier.

    Args:
        label: Label du modèle (ex: '4 stars', '1 star')

    Returns:
        int: Score de sentiment entre 1 et 5
    """
    return int(label.split()[0])


def compute_coherence(note_allocine: float, sentiment_score: int) -> str:
    """
    Détermine la cohérence entre la note Allociné et le sentiment NLP.

    Un écart absolu <= 1 est considéré comme cohérent.

    Args:
        note_allocine:   Note sur 5 donnée par l'utilisateur sur Allociné
        sentiment_score: Score de sentiment (1 à 5) prédit par le modèle NLP

    Returns:
        str: 'coherent', 'sur-estime' ou 'sous-estime'
    """
    diff = abs(note_allocine - sentiment_score)
    if diff <= 1:
        return "coherent"
    elif note_allocine > sentiment_score:
        return "sur-estime"
    else:
        return "sous-estime"


# ── Orchestration ──────────────────────────────────────────────────────────────

def run_transformations() -> pd.DataFrame:
    """
    Orchestre le nettoyage et l'analyse de sentiment sur les avis scrappés.

    Étapes :
        1. Chargement du CSV brut
        2. Nettoyage et typage
        3. Analyse de sentiment NLP par batch
        4. Calcul de la cohérence note / sentiment
        5. Sauvegarde dans data/processed/reviews_clean.csv

    Returns:
        pd.DataFrame: Table enrichie avec scores de sentiment et cohérence
    """
    csv_path = RAW_DATA_PATH / "reviews_raw.csv"
    if not csv_path.exists():
        logger.error("Fichier introuvable : %s — lancer d'abord scraper.py", csv_path)
        raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info("%d avis bruts chargés depuis '%s'", len(df), csv_path)

    # Nettoyage
    df = clean_reviews(df)

    # Analyse de sentiment
    logger.info("Démarrage de l'analyse de sentiment (%d avis)...", len(df))
    model = load_sentiment_model()
    predictions = predict_sentiment_batch(df["texte"].tolist(), model)

    df["sentiment_label"]      = [p["label"] for p in predictions]
    df["sentiment_score"]      = [parse_sentiment_label(p["label"]) for p in predictions]
    df["sentiment_confidence"] = [round(p["score"], 4) for p in predictions]

    # Cohérence note / sentiment
    df["coherence"] = df.apply(
        lambda row: compute_coherence(row["note"], row["sentiment_score"]), axis=1
    )

    # Sélection des colonnes finales
    df = df[COLONNES_FINALES]

    # Sauvegarde
    output_path = PROCESSED_DATA_PATH / "reviews_clean.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    logger.info("%d avis transformés sauvegardés dans '%s'", len(df), output_path)
    logger.info(
        "Répartition cohérence : %s",
        df["coherence"].value_counts().to_dict(),
    )
    logger.info(
        "Score sentiment moyen par contenu :\n%s",
        df.groupby("content_name")["sentiment_score"].mean().round(2).to_string(),
    )

    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_transformations()