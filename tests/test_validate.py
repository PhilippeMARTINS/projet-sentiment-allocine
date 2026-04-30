"""
tests/test_validate.py
----------------------
Tests unitaires pour src/validate.py — Projet 3 : Sentiment Allociné.

Fonctions testées :
    - validate_raw_reviews   : validation des avis bruts scrappés
    - validate_clean_reviews : validation des avis après transformation NLP

Lancer avec : pytest tests/ -v
"""

import pytest
import pandas as pd
from src.validate import validate_raw_reviews, validate_clean_reviews


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def df_raw_valide() -> pd.DataFrame:
    """DataFrame brut minimal et valide (10 avis sur 5 contenus distincts)."""
    lignes = []
    contenus = [
        ("182745", "film",   "Intouchables"),
        ("143692", "film",   "Inception"),
        ("604",    "series", "Game of Thrones"),
        ("3517",   "series", "Breaking Bad"),
        ("61282",  "film",   "Interstellar"),
    ]
    for content_id, content_type, content_name in contenus:
        for i in range(2):
            lignes.append({
                "content_id":   content_id,
                "content_type": content_type,
                "content_name": content_name,
                "titre_avis":   f"Avis {i}",
                "note":         float(3 + i),
                "texte":        f"Texte de l'avis numéro {i} pour {content_name}.",
                "date":         "01 janvier 2023",
                "scraped_at":   "2024-01-01T10:00:00",
            })
    return pd.DataFrame(lignes)


@pytest.fixture
def df_clean_valide(df_raw_valide) -> pd.DataFrame:
    """DataFrame nettoyé et enrichi valide."""
    df = df_raw_valide.copy()
    df["date_clean"]           = "2023-01-01"
    df["sentiment_label"]      = "4 stars"
    df["sentiment_score"]      = 4
    df["sentiment_confidence"] = 0.85
    df["coherence"]            = "coherent"
    df = df.drop(columns=["date"])
    return df


# ── Tests : validate_raw_reviews ─────────────────────────────────────────────

class TestValidateRawReviews:

    def test_dataframe_valide_passe(self, df_raw_valide):
        """Un DataFrame valide doit passer toutes les validations."""
        assert validate_raw_reviews(df_raw_valide) is True

    def test_dataframe_vide_echoue(self):
        """Un DataFrame vide doit échouer immédiatement."""
        assert validate_raw_reviews(pd.DataFrame()) is False

    def test_note_hors_plage_echoue(self, df_raw_valide):
        """Une note hors de [0.5, 5.0] doit faire échouer la validation."""
        df = df_raw_valide.copy()
        df.loc[0, "note"] = 6.0
        assert validate_raw_reviews(df) is False

    def test_content_type_invalide_echoue(self, df_raw_valide):
        """Un content_type inconnu doit faire échouer la validation."""
        df = df_raw_valide.copy()
        df.loc[0, "content_type"] = "podcast"
        assert validate_raw_reviews(df) is False

    def test_moins_de_5_contenus_echoue(self, df_raw_valide):
        """Moins de 5 contenus distincts doit faire échouer la validation."""
        df = df_raw_valide[df_raw_valide["content_name"] == "Intouchables"].copy()
        assert validate_raw_reviews(df) is False

    def test_colonne_manquante_echoue(self, df_raw_valide):
        """Un DataFrame sans la colonne 'note' doit échouer."""
        df = df_raw_valide.drop(columns=["note"])
        assert validate_raw_reviews(df) is False


# ── Tests : validate_clean_reviews ───────────────────────────────────────────

class TestValidateCleanReviews:

    def test_dataframe_valide_passe(self, df_clean_valide):
        """Un DataFrame nettoyé valide doit passer toutes les validations."""
        assert validate_clean_reviews(df_clean_valide) is True

    def test_sentiment_score_hors_plage_echoue(self, df_clean_valide):
        """Un score sentiment hors de [1, 5] doit faire échouer la validation."""
        df = df_clean_valide.copy()
        df.loc[0, "sentiment_score"] = 6
        assert validate_clean_reviews(df) is False

    def test_coherence_invalide_echoue(self, df_clean_valide):
        """Une valeur de cohérence inconnue doit faire échouer la validation."""
        df = df_clean_valide.copy()
        df.loc[0, "coherence"] = "inconnu"
        assert validate_clean_reviews(df) is False

    def test_confidence_hors_plage_echoue(self, df_clean_valide):
        """Une confidence > 1 doit faire échouer la validation."""
        df = df_clean_valide.copy()
        df.loc[0, "sentiment_confidence"] = 1.5
        assert validate_clean_reviews(df) is False

    def test_doublons_echouent(self, df_clean_valide):
        """Un DataFrame avec des lignes dupliquées doit échouer."""
        df = pd.concat([df_clean_valide, df_clean_valide], ignore_index=True)
        assert validate_clean_reviews(df) is False