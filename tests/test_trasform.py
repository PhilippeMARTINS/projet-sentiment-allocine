"""
tests/test_transform.py
-----------------------
Tests unitaires pour src/transform.py — Projet 3 : Sentiment Allociné.

Fonctions testées :
    - parse_sentiment_label   : conversion label '4 stars' → entier 4
    - compute_coherence       : détection écart note / sentiment
    - clean_reviews           : nettoyage et typage du DataFrame brut

Note : les tests NLP (predict_sentiment_batch) ne sont PAS testés ici
car ils nécessitent le téléchargement du modèle HuggingFace (~700 Mo).
Ils sont couverts par des tests d'intégration séparés.

Lancer avec : pytest tests/ -v
"""

import pytest
import pandas as pd
from src.transform import parse_sentiment_label, compute_coherence, clean_reviews


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_reviews_raw() -> pd.DataFrame:
    """DataFrame d'avis bruts minimal pour les tests de clean_reviews."""
    return pd.DataFrame({
        "content_id":   ["182745", "143692", "604"],
        "content_type": ["film",   "film",   "series"],
        "content_name": ["Intouchables", "Inception", "Game of Thrones"],
        "titre_avis":   ["Super film", None, "Excellent"],
        "note":         [4.5, 3.0, None],
        "texte":        ["Un film magnifique.", "Bien mais trop long.", "Série incroyable !"],
        "date":         ["12 janvier 2023", "3 mars 2022", None],
        "scraped_at":   ["2024-01-01T10:00:00", "2024-01-01T10:01:00", "2024-01-01T10:02:00"],
    })


# ── Tests : parse_sentiment_label ─────────────────────────────────────────────

class TestParseSentimentLabel:
    """Tests pour la fonction parse_sentiment_label."""

    def test_un_star_retourne_1(self):
        """'1 star' doit retourner l'entier 1."""
        assert parse_sentiment_label("1 star") == 1

    def test_cinq_stars_retourne_5(self):
        """'5 stars' doit retourner l'entier 5."""
        assert parse_sentiment_label("5 stars") == 5

    def test_quatre_stars_retourne_4(self):
        """'4 stars' doit retourner l'entier 4."""
        assert parse_sentiment_label("4 stars") == 4

    def test_deux_stars_retourne_2(self):
        """'2 stars' doit retourner l'entier 2."""
        assert parse_sentiment_label("2 stars") == 2

    def test_retourne_entier(self):
        """La valeur retournée doit être de type int."""
        result = parse_sentiment_label("3 stars")
        assert isinstance(result, int), f"Attendu int, obtenu {type(result)}"

    def test_tous_labels_valides(self):
        """Tous les labels HuggingFace valides doivent être convertis sans erreur."""
        labels = ["1 star", "2 stars", "3 stars", "4 stars", "5 stars"]
        resultats = [parse_sentiment_label(label) for label in labels]
        assert resultats == [1, 2, 3, 4, 5]

    def test_labels_strictement_croissants(self):
        """Les scores convertis doivent être strictement croissants."""
        labels = ["1 star", "2 stars", "3 stars", "4 stars", "5 stars"]
        scores = [parse_sentiment_label(label) for label in labels]
        assert scores == sorted(scores), "Les scores doivent être croissants"


# ── Tests : compute_coherence ─────────────────────────────────────────────────

class TestComputeCoherence:
    """Tests pour la fonction compute_coherence."""

    def test_coherent_si_ecart_faible(self):
        """Un écart <= 1 doit retourner 'coherent'."""
        assert compute_coherence(4.0, 4) == "coherent"
        assert compute_coherence(4.0, 3) == "coherent"
        assert compute_coherence(4.0, 5) == "coherent"

    def test_sur_estime_si_note_superieure_au_sentiment(self):
        """Note > sentiment + 1 : l'utilisateur sur-estime."""
        assert compute_coherence(5.0, 2) == "sur-estime"
        assert compute_coherence(4.0, 1) == "sur-estime"

    def test_sous_estime_si_note_inferieure_au_sentiment(self):
        """Note < sentiment - 1 : l'utilisateur sous-estime."""
        assert compute_coherence(1.0, 4) == "sous-estime"
        assert compute_coherence(2.0, 5) == "sous-estime"

    def test_ecart_exactement_1_est_coherent(self):
        """Un écart exactement égal à 1 doit rester 'coherent'."""
        assert compute_coherence(3.0, 4) == "coherent"
        assert compute_coherence(4.0, 3) == "coherent"

    def test_retourne_string(self):
        """La valeur retournée doit être une chaîne de caractères."""
        result = compute_coherence(3.0, 3)
        assert isinstance(result, str)

    def test_valeurs_egales_sont_coherentes(self):
        """Une note identique au sentiment est forcément cohérente."""
        for val in [1.0, 2.0, 3.0, 4.0, 5.0]:
            assert compute_coherence(val, int(val)) == "coherent"

    @pytest.mark.parametrize("note,sentiment,attendu", [
        (5.0, 5, "coherent"),
        (4.5, 4, "coherent"),
        (1.0, 5, "sous-estime"),
        (5.0, 1, "sur-estime"),
        (3.0, 3, "coherent"),
        (2.0, 4, "sous-estime"),
    ])
    def test_cas_parametrises(self, note, sentiment, attendu):
        """Vérifie la cohérence sur un ensemble de cas représentatifs."""
        assert compute_coherence(note, sentiment) == attendu


# ── Tests : clean_reviews ─────────────────────────────────────────────────────

class TestCleanReviews:
    """Tests pour la fonction clean_reviews."""

    def test_retourne_dataframe(self, sample_reviews_raw):
        """clean_reviews doit retourner un DataFrame."""
        result = clean_reviews(sample_reviews_raw)
        assert isinstance(result, pd.DataFrame)

    def test_supprime_lignes_sans_texte(self, sample_reviews_raw):
        """Les lignes sans texte doivent être supprimées."""
        # Ajouter une ligne sans texte
        df_avec_vide = sample_reviews_raw.copy()
        df_avec_vide.loc[3] = {
            "content_id": "999", "content_type": "film", "content_name": "Test",
            "titre_avis": None, "note": 3.0, "texte": None,
            "date": "01 janvier 2023", "scraped_at": "2024-01-01T10:03:00",
        }
        result = clean_reviews(df_avec_vide)
        assert result["texte"].notna().all(), "Aucun texte vide ne doit subsister"

    def test_colonne_date_clean_creee(self, sample_reviews_raw):
        """La colonne 'date_clean' doit être créée."""
        result = clean_reviews(sample_reviews_raw)
        assert "date_clean" in result.columns, "Colonne 'date_clean' manquante"

    def test_note_entre_0_et_5(self, sample_reviews_raw):
        """Les notes valides doivent être comprises entre 0 et 5."""
        result = clean_reviews(sample_reviews_raw)
        notes = result["note"].dropna()
        assert notes.between(0, 5).all(), f"Notes hors plage : {notes[~notes.between(0, 5)]}"

    def test_colonnes_originales_conservees(self, sample_reviews_raw):
        """Les colonnes essentielles du DataFrame brut doivent être présentes."""
        result = clean_reviews(sample_reviews_raw)
        for col in ["content_id", "content_type", "content_name", "note", "texte"]:
            assert col in result.columns, f"Colonne manquante : {col}"