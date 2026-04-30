"""
analyze.py
----------
Module d'analyse : génération des 5 visualisations Allociné
(note vs sentiment, cohérence, distribution notes, sentiment par type, heatmap).
"""

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import seaborn as sns


# ── Configuration ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)

PROCESSED_PATH = Path("data/processed")
OUTPUT_PATH    = Path("outputs")
OUTPUT_PATH.mkdir(exist_ok=True)

sns.set_theme(style="whitegrid")

COULEURS = {
    "film":   "#2563EB",
    "series": "#16A34A",
}


# ── Chargement ────────────────────────────────────────────────────────────────

def load_data() -> pd.DataFrame:
    """
    Charge les avis transformés depuis data/processed/reviews_clean.csv.

    Returns:
        pd.DataFrame: Avis enrichis avec scores de sentiment

    Raises:
        FileNotFoundError: Si le fichier reviews_clean.csv est absent
    """
    csv_path = PROCESSED_PATH / "reviews_clean.csv"
    if not csv_path.exists():
        logger.error("Fichier introuvable : %s — lancer d'abord transform.py", csv_path)
        raise FileNotFoundError(f"Fichier introuvable : {csv_path}")

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info("%d avis chargés pour l'analyse", len(df))
    return df


# ── Visualisations ────────────────────────────────────────────────────────────

def plot_note_vs_sentiment(df: pd.DataFrame) -> None:
    """
    Graphique en barres groupées : note Allociné vs score sentiment NLP par contenu.
    C'est le graphique central du projet (insight note/sentiment).

    Args:
        df: DataFrame issu de load_data()
    """
    stats = (
        df.groupby("content_name")
        .agg(note_moyenne=("note", "mean"), sentiment_moyen=("sentiment_score", "mean"))
        .round(2)
        .reset_index()
    )

    x     = range(len(stats))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))
    bars1 = ax.bar(
        [i - width / 2 for i in x], stats["note_moyenne"],
        width, label="Note Allociné", color="#2563EB", alpha=0.85,
    )
    bars2 = ax.bar(
        [i + width / 2 for i in x], stats["sentiment_moyen"],
        width, label="Score Sentiment NLP", color="#DC2626", alpha=0.85,
    )

    ax.set_xticks(list(x))
    ax.set_xticklabels(stats["content_name"], rotation=20, ha="right", fontsize=10)
    ax.set_ylabel("Score (/5)")
    ax.set_ylim(0, 5.5)
    ax.set_title("Note Allociné vs Score Sentiment NLP par contenu",
                 fontsize=14, fontweight="bold")
    ax.legend()

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{bar.get_height():.2f}", ha="center", va="bottom", fontsize=8)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{bar.get_height():.2f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "note_vs_sentiment.png", dpi=150)
    plt.close()
    logger.info("Visualisation sauvegardée : note_vs_sentiment.png")


def plot_coherence(df: pd.DataFrame) -> None:
    """
    Camembert de la distribution cohérence note/sentiment (cohérent, sur-estime, sous-estime).

    Args:
        df: DataFrame issu de load_data()
    """
    counts = df["coherence"].value_counts()
    colors = {"coherent": "#16A34A", "sur-estime": "#DC2626", "sous-estime": "#D97706"}
    couleurs_liste = [colors.get(c, "#6B7280") for c in counts.index]

    fig, ax = plt.subplots(figsize=(8, 6))
    wedges, texts, autotexts = ax.pie(
        counts,
        labels=counts.index,
        colors=couleurs_liste,
        autopct="%1.1f%%",
        startangle=140,
        pctdistance=0.8,
    )
    for autotext in autotexts:
        autotext.set_fontsize(11)
        autotext.set_fontweight("bold")

    ax.set_title("Cohérence note Allociné / sentiment NLP",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "coherence_note_sentiment.png", dpi=150)
    plt.close()
    logger.info("Visualisation sauvegardée : coherence_note_sentiment.png")


def plot_distribution_notes(df: pd.DataFrame) -> None:
    """
    Histogramme de la distribution des notes Allociné (1 à 5).

    Args:
        df: DataFrame issu de load_data()
    """
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.histplot(df["note"].dropna(), bins=9, kde=True, color="#2563EB", ax=ax)
    ax.set_xlabel("Note (/5)")
    ax.set_ylabel("Nombre d'avis")
    ax.set_title("Distribution des notes Allociné", fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "distribution_notes.png", dpi=150)
    plt.close()
    logger.info("Visualisation sauvegardée : distribution_notes.png")


def plot_sentiment_par_type(df: pd.DataFrame) -> None:
    """
    Barres horizontales du score sentiment moyen par contenu, colorées par type (film/série).

    Args:
        df: DataFrame issu de load_data()
    """
    stats = (
        df.groupby(["content_name", "content_type"])["sentiment_score"]
        .mean()
        .round(2)
        .reset_index()
        .sort_values("sentiment_score", ascending=False)
    )

    couleurs = [COULEURS.get(t, "#6B7280") for t in stats["content_type"]]

    fig, ax = plt.subplots(figsize=(12, 5))
    bars = ax.barh(stats["content_name"], stats["sentiment_score"],
                   color=couleurs, alpha=0.85)

    for bar, val in zip(bars, stats["sentiment_score"]):
        ax.text(bar.get_width() + 0.03, bar.get_y() + bar.get_height() / 2,
                f"{val:.2f}", va="center", fontsize=10, fontweight="bold")

    ax.set_xlabel("Score sentiment moyen (/5)")
    ax.set_title("Score sentiment NLP moyen par contenu",
                 fontsize=14, fontweight="bold")
    ax.set_xlim(0, 5.5)

    legend_elements = [
        mpatches.Patch(facecolor="#2563EB", label="Film"),
        mpatches.Patch(facecolor="#16A34A", label="Série"),
    ]
    ax.legend(handles=legend_elements, loc="lower right")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "sentiment_par_contenu.png", dpi=150)
    plt.close()
    logger.info("Visualisation sauvegardée : sentiment_par_contenu.png")


def plot_heatmap_coherence(df: pd.DataFrame) -> None:
    """
    Heatmap du taux de cohérence note/sentiment par contenu (en pourcentage).

    Args:
        df: DataFrame issu de load_data()
    """
    pivot = df.groupby(["content_name", "coherence"]).size().unstack(fill_value=0)
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0) * 100

    fig, ax = plt.subplots(figsize=(10, 6))
    sns.heatmap(pivot_pct, annot=True, fmt=".1f", cmap="RdYlGn",
                linewidths=0.5, ax=ax, cbar_kws={"label": "%"})
    ax.set_title("Cohérence note/sentiment par contenu (%)",
                 fontsize=14, fontweight="bold")
    ax.set_xlabel("Cohérence")
    ax.set_ylabel("Contenu")
    plt.tight_layout()
    plt.savefig(OUTPUT_PATH / "heatmap_coherence.png", dpi=150)
    plt.close()
    logger.info("Visualisation sauvegardée : heatmap_coherence.png")


# ── Orchestration ──────────────────────────────────────────────────────────────

def run_analysis() -> None:
    """
    Lance la génération des 5 visualisations et les sauvegarde dans outputs/.

    Visualisations produites :
        - note_vs_sentiment.png
        - coherence_note_sentiment.png
        - distribution_notes.png
        - sentiment_par_contenu.png
        - heatmap_coherence.png
    """
    logger.info("Génération des visualisations...")
    df = load_data()
    plot_note_vs_sentiment(df)
    plot_coherence(df)
    plot_distribution_notes(df)
    plot_sentiment_par_type(df)
    plot_heatmap_coherence(df)
    logger.info("Toutes les visualisations sont dans '%s'", OUTPUT_PATH)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    run_analysis()