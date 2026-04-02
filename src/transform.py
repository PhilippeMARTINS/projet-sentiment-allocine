"""
transform.py
------------
Module de transformation : nettoyage des avis et analyse de sentiment
avec le modèle CamemBERT (modèle NLP français de HuggingFace).
"""

import pandas as pd
import re
from pathlib import Path
from transformers import pipeline
from typing import Optional


RAW_DATA_PATH       = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")
PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)


def clean_text(text: str) -> str:
    """
    Nettoie le texte d'un avis.

    Args:
        text: Texte brut

    Returns:
        str: Texte nettoyé
    """
    if not isinstance(text, str):
        return ""
    # Suppression des espaces multiples et caractères spéciaux
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[^\w\s\.,!?;:'\"-]", " ", text)
    return text


def clean_date(date_str: str) -> Optional[str]:
    """
    Nettoie et normalise la date au format YYYY-MM-DD.

    Args:
        date_str: Date brute (ex: 'Publiée le 17 octobre 2013')

    Returns:
        Optional[str]: Date normalisée ou None
    """
    if not isinstance(date_str, str):
        return None

    mois = {
        "janvier": "01", "février": "02", "mars": "03",
        "avril": "04", "mai": "05", "juin": "06",
        "juillet": "07", "août": "08", "septembre": "09",
        "octobre": "10", "novembre": "11", "décembre": "12"
    }

    try:
        # Format : "Publiée le 17 octobre 2013"
        parts = date_str.replace("Publiée le ", "").strip().split()
        if len(parts) == 3:
            jour, mois_str, annee = parts
            return f"{annee}-{mois.get(mois_str, '01')}-{jour.zfill(2)}"
    except Exception:
        pass
    return None


def load_sentiment_model():
    """
    Charge le modèle de sentiment analysis pour le français.
    Utilise 'nlptown/bert-base-multilingual-uncased-sentiment'
    qui supporte le français et retourne une note de 1 à 5 étoiles.

    Returns:
        pipeline: Modèle HuggingFace prêt à l'emploi
    """
    print("⏳ Chargement du modèle NLP...")
    model = pipeline(
        "sentiment-analysis",
        model="nlptown/bert-base-multilingual-uncased-sentiment",
        truncation=True,
        max_length=512,
    )
    print("✅ Modèle NLP chargé")
    return model


def predict_sentiment_batch(texts: list[str], model, batch_size: int = 32) -> list[dict]:
    """
    Prédit le sentiment pour une liste de textes par batch.

    Args:
        texts: Liste de textes à analyser
        model: Modèle HuggingFace
        batch_size: Taille des batches pour l'inférence

    Returns:
        list[dict]: Liste de {'label': '4 stars', 'score': 0.87}
    """
    results = []
    total = len(texts)

    for i in range(0, total, batch_size):
        batch = texts[i:i + batch_size]
        try:
            preds = model(batch)
            results.extend(preds)
        except Exception as e:
            print(f"   ⚠️ Erreur batch {i}-{i+batch_size} : {e}")
            results.extend([{"label": "3 stars", "score": 0.0}] * len(batch))

        if (i // batch_size) % 5 == 0:
            print(f"   Progression : {min(i + batch_size, total)}/{total} avis traités")

    return results


def parse_sentiment_label(label: str) -> int:
    """
    Convertit le label '1 star' à '5 stars' en entier.

    Args:
        label: Label du modèle (ex: '4 stars')

    Returns:
        int: Note de sentiment (1 à 5)
    """
    return int(label.split()[0])


def compute_coherence(note_allocine: float, sentiment_score: int) -> str:
    """
    Calcule la cohérence entre la note Allociné et le sentiment NLP.

    Args:
        note_allocine: Note sur 5 donnée par l'utilisateur
        sentiment_score: Score de sentiment (1 à 5) prédit par le modèle

    Returns:
        str: 'coherent', 'sous-estime' ou 'sur-estime'
    """
    diff = abs(note_allocine - sentiment_score)
    if diff <= 1:
        return "coherent"
    elif note_allocine > sentiment_score:
        return "sur-estime"
    else:
        return "sous-estime"


def run_transformations() -> pd.DataFrame:
    """
    Orchestre le nettoyage et l'analyse de sentiment sur les avis scrappés.

    Returns:
        pd.DataFrame: Table enrichie avec scores de sentiment
    """
    print("=== TRANSFORMATION ===")

    # Chargement des données brutes
    df = pd.read_csv(RAW_DATA_PATH / "reviews_raw.csv", encoding="utf-8-sig")
    print(f"✅ {len(df)} avis chargés")

    # Nettoyage
    df["texte"]      = df["texte"].apply(clean_text)
    df["date_clean"] = df["date"].apply(clean_date)
    df["note"]       = pd.to_numeric(df["note"], errors="coerce")

    # Suppression des lignes sans texte ou sans note
    df = df.dropna(subset=["texte", "note"])
    df = df[df["texte"].str.len() > 10]
    print(f"✅ {len(df)} avis après nettoyage")

    # Analyse de sentiment
    print("\n⏳ Analyse de sentiment en cours...")
    model = load_sentiment_model()
    textes = df["texte"].tolist()
    predictions = predict_sentiment_batch(textes, model)

    df["sentiment_label"] = [p["label"] for p in predictions]
    df["sentiment_score"] = [parse_sentiment_label(p["label"]) for p in predictions]
    df["sentiment_confidence"] = [round(p["score"], 4) for p in predictions]

    # Cohérence note / sentiment
    df["coherence"] = df.apply(
        lambda row: compute_coherence(row["note"], row["sentiment_score"]), axis=1
    )

    # Colonnes finales
    df = df[[
        "content_id", "content_type", "content_name",
        "titre_avis", "note", "texte", "date_clean",
        "sentiment_label", "sentiment_score", "sentiment_confidence",
        "coherence", "scraped_at"
    ]]

    # Sauvegarde locale
    output_path = PROCESSED_DATA_PATH / "reviews_clean.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\n✅ {len(df)} avis transformés sauvegardés dans {output_path}")
    print(f"\n📊 Répartition cohérence note/sentiment :")
    print(df["coherence"].value_counts())
    print(f"\n📊 Score sentiment moyen par contenu :")
    print(df.groupby("content_name")["sentiment_score"].mean().round(2))

    return df


if __name__ == "__main__":
    df = run_transformations()