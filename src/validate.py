"""
validate.py
-----------
Validation de la qualité des avis scrappés et des données transformées.
Appelé automatiquement par main.py après le scraping et la transformation.
"""

import logging
import pandas as pd


logger = logging.getLogger(__name__)

# ── Constantes de référence ───────────────────────────────────────────────────
COLONNES_RAW = ["content_id", "content_type", "content_name",
                "titre_avis", "note", "texte", "date", "scraped_at"]

COLONNES_CLEAN = ["content_id", "content_type", "content_name",
                  "titre_avis", "note", "texte", "date_clean",
                  "sentiment_label", "sentiment_score", "sentiment_confidence",
                  "coherence", "scraped_at"]

COHERENCES_ATTENDUES   = {"coherent", "sur-estime", "sous-estime"}
CONTENT_TYPES_ATTENDUS = {"film", "series"}
LABELS_SENTIMENT       = {"1 star", "2 stars", "3 stars", "4 stars", "5 stars"}
TAUX_TEXTE_MIN         = 0.80


def _check(label: str, condition: bool, detail: str = "") -> bool:
    """
    Logue le résultat d'un check et retourne le succès en bool Python natif.

    Args:
        label:     Libellé du check
        condition: True si le check passe
        detail:    Message complémentaire en cas d'échec

    Returns:
        bool: True (Python natif) si le check passe, False sinon
    """
    # Conversion explicite en bool Python natif pour éviter numpy.bool_
    result = bool(condition)
    if result:
        logger.info("    [OK] %s", label)
    else:
        msg = f"    [KO] {label}"
        if detail:
            msg += f" -- {detail}"
        logger.warning(msg)
    return result


def validate_raw_reviews(df: pd.DataFrame) -> bool:
    """
    Valide les avis bruts issus du scraping.

    Args:
        df: DataFrame issu de run_scraping()

    Returns:
        bool: True si toutes les validations passent
    """
    logger.info("Validation des avis bruts scrappés :")
    all_passed = True

    all_passed &= _check(
        "DataFrame non vide",
        len(df) > 0,
        f"{len(df)} lignes",
    )
    if df.empty:
        return False

    # Colonnes — vérification en premier avant tout accès
    for col in COLONNES_RAW:
        col_present = _check(f"colonne '{col}' presente", col in df.columns)
        all_passed &= col_present

    # Types de contenu
    if "content_type" in df.columns:
        all_passed &= _check(
            "content_type dans {film, series}",
            set(df["content_type"].unique()).issubset(CONTENT_TYPES_ATTENDUS),
            f"valeurs inattendues : {set(df['content_type'].unique()) - CONTENT_TYPES_ATTENDUS}",
        )

    # Notes — seulement si la colonne existe
    if "note" in df.columns:
        notes_valides = df["note"].dropna()
        all_passed &= _check(
            "notes entre 0.5 et 5.0",
            bool(notes_valides.between(0.5, 5.0).all()),
            f"min={notes_valides.min():.1f}, max={notes_valides.max():.1f}",
        )

    # Textes non vides
    if "texte" in df.columns:
        taux_texte = df["texte"].notna().mean()
        all_passed &= _check(
            f"taux de textes non vides >= {TAUX_TEXTE_MIN:.0%}",
            bool(taux_texte >= TAUX_TEXTE_MIN),
            f"observe : {taux_texte:.1%}",
        )

    # Au moins 5 contenus distincts
    if "content_name" in df.columns:
        all_passed &= _check(
            "au moins 5 contenus distincts scrappés",
            df["content_name"].nunique() >= 5,
            f"observe : {df['content_name'].nunique()} contenus",
        )

    if all_passed:
        logger.info("Validation avis bruts : toutes les verifications sont passees [OK]")
    else:
        logger.warning("Validation avis bruts : certains checks ont echoue [KO]")

    return bool(all_passed)


def validate_clean_reviews(df: pd.DataFrame) -> bool:
    """
    Valide les avis après transformation et analyse de sentiment NLP.

    Args:
        df: DataFrame issu de run_transformations()

    Returns:
        bool: True si toutes les validations passent
    """
    logger.info("Validation des avis transformés :")
    all_passed = True

    # Colonnes
    for col in COLONNES_CLEAN:
        all_passed &= _check(f"colonne '{col}' presente", col in df.columns)

    if "sentiment_label" in df.columns:
        all_passed &= _check(
            "sentiment_label dans les valeurs attendues",
            bool(set(df["sentiment_label"].unique()).issubset(LABELS_SENTIMENT)),
            f"valeurs inattendues : {set(df['sentiment_label'].unique()) - LABELS_SENTIMENT}",
        )

    if "sentiment_score" in df.columns:
        all_passed &= _check(
            "sentiment_score entre 1 et 5",
            bool(df["sentiment_score"].between(1, 5).all()),
            f"min={df['sentiment_score'].min()}, max={df['sentiment_score'].max()}",
        )

    if "sentiment_confidence" in df.columns:
        all_passed &= _check(
            "sentiment_confidence entre 0 et 1",
            bool(df["sentiment_confidence"].between(0, 1).all()),
        )

    if "coherence" in df.columns:
        all_passed &= _check(
            "coherence dans {coherent, sur-estime, sous-estime}",
            bool(set(df["coherence"].unique()).issubset(COHERENCES_ATTENDUES)),
            f"valeurs inattendues : {set(df['coherence'].unique()) - COHERENCES_ATTENDUES}",
        )
        taux_coherent = (df["coherence"] == "coherent").mean()
        logger.info("  Taux de coherence note/sentiment : %.1f%%", taux_coherent * 100)

    all_passed &= _check(
        "aucune ligne dupliquee",
        bool(df.duplicated().sum() == 0),
        f"{df.duplicated().sum()} doublons detectes",
    )

    if all_passed:
        logger.info("Validation avis transformes : toutes les verifications sont passees [OK]")
    else:
        logger.warning("Validation avis transformes : certains checks ont echoue [KO]")

    return bool(all_passed)